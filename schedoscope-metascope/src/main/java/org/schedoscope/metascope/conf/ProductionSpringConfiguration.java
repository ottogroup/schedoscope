/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.conf;

import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.service.UserEntityService;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.tasks.repository.jdbc.NativeSqlRepository;
import org.schedoscope.metascope.tasks.repository.mysql.MySQLRepository;
import org.schedoscope.metascope.util.ValidationQueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.embedded.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.authentication.encoding.ShaPasswordEncoder;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.session.HttpSessionEventPublisher;

import com.typesafe.config.ConfigFactory;

/**
 * Configuration to secure the application (login)
 */
@Configuration
@ComponentScan
@EnableAsync
@Profile("production")
public class ProductionSpringConfiguration extends WebSecurityConfigurerAdapter {

  @Autowired
  private UserEntityService userEntityService;

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    MetascopeConfig config = metascopeConfig();
    if (config.getAuthenticationMethod().equalsIgnoreCase("ldap")) {
      String[] allgroups = appendRolePrefix(config.getAllowedGroups(), config.getAdminGroups());
      String[] adminGroups = appendRolePrefix(config.getAdminGroups());
      http.authorizeRequests()
          .antMatchers("/", "/?error=cred", "/status/*", "/expired").permitAll()
          .antMatchers("/admin**").hasAnyAuthority(adminGroups)
          .antMatchers("/admin/").hasAnyAuthority(adminGroups)
          .antMatchers("/admin/**").hasAnyAuthority(adminGroups)
          .antMatchers("/**").hasAnyAuthority(allgroups)
          .anyRequest().authenticated()
          .and()
          .formLogin().loginPage("/").failureUrl("/?error=cred")
          .defaultSuccessUrl("/home")
          .and()
          .logout().logoutSuccessUrl("/").and().rememberMe().and().exceptionHandling()
          .accessDeniedPage("/accessdenied");
    } else {
      http.authorizeRequests().antMatchers("/", "/?error=cred", "/status/*", "/expired").permitAll()
          .antMatchers("/admin**").hasAuthority("ROLE_ADMIN").antMatchers("/admin/").hasAuthority("ROLE_ADMIN")
          .antMatchers("/admin/**").hasAuthority("ROLE_ADMIN").anyRequest().authenticated().and().formLogin()
          .loginPage("/").failureUrl("/?error=cred").and().logout().logoutSuccessUrl("/").and().rememberMe().and()
          .exceptionHandling().accessDeniedPage("/accessdenied");
    }
    http.sessionManagement().maximumSessions(1).expiredUrl("/expired").sessionRegistry(sessionRegistry());
  }

  @Override
  protected void configure(AuthenticationManagerBuilder auth) throws Exception {
    MetascopeConfig config = metascopeConfig();
    if (config.getAuthenticationMethod().equalsIgnoreCase("ldap")) {
      auth.ldapAuthentication().userDnPatterns(config.getUserDnPattern()).groupSearchBase(config.getGroupSearchBase())
          .contextSource(ldapContextSource());
    } else {
      auth.jdbcAuthentication().passwordEncoder(new ShaPasswordEncoder(256)).dataSource(dataSource())
          .usersByUsernameQuery("select username,password_hash,'true' from user_entity where username = ?")
          .authoritiesByUsernameQuery("select username,userrole from user_entity where username = ?");

      userEntityService.createAdminAccount();
    }

  }

  @Bean
  public LdapContextSource ldapContextSource() {
    MetascopeConfig config = metascopeConfig();
    LdapContextSource contextSource = new LdapContextSource();
    contextSource.setUrl(config.getLdapUrl());
    contextSource.setUserDn(config.getManagerDn());
    contextSource.setPassword(config.getManagerPassword());
    return contextSource;
  }

  @Bean
  public LdapTemplate ldapTemplate() {
    return new LdapTemplate(ldapContextSource());
  }

  @Bean
  public SessionRegistry sessionRegistry() {
    return new SessionRegistryImpl();
  }

  @Bean
  public ServletListenerRegistrationBean<HttpSessionEventPublisher> httpSessionEventPublisher() {
    return new ServletListenerRegistrationBean<HttpSessionEventPublisher>(new HttpSessionEventPublisher());
  }

  @Bean
  public MetascopeConfig metascopeConfig() {
    return new MetascopeConfig(new BaseSettings(ConfigFactory.load()));
  }

  @Bean
  public DataSource dataSource() {
    MetascopeConfig metascopeConfig = metascopeConfig();
    DataSource dataSource = DataSourceBuilder.create().username(metascopeConfig.getRepositoryUser())
        .password(metascopeConfig.getRepositoryPassword()).url(metascopeConfig.getRepositoryUrl()).build();

    if (dataSource instanceof org.apache.tomcat.jdbc.pool.DataSource) {
      org.apache.tomcat.jdbc.pool.DataSource tomcatDataSource = (org.apache.tomcat.jdbc.pool.DataSource) dataSource;
      tomcatDataSource.setTestOnBorrow(true);
      tomcatDataSource.setMaxIdle(10);
      tomcatDataSource.setMinIdle(1);
      tomcatDataSource.setTestWhileIdle(true);
      String validationQuery = ValidationQueryUtil.getValidationQuery(tomcatDataSource.getDriverClassName());
      tomcatDataSource.setValidationQuery(validationQuery);
    }

    return dataSource;
  }

  @Bean
  public RepositoryDAO repositoryDAO() {
    MetascopeConfig config = metascopeConfig();
    if (config.getRepositoryUrl().startsWith("jdbc:mysql")) {
      return new MySQLRepository();
    } else {
      return new NativeSqlRepository();
    }
  }

  @Bean
  public SolrFacade solrFacade() {
    MetascopeConfig config = metascopeConfig();
    return new SolrFacade(config.getSolrUrl());
  }

  private String[] appendRolePrefix(String groups) {
    String[] groupsArray = groups.split(",");
    for (int i = 0; i < groupsArray.length; i++) {
      groupsArray[i] = "ROLE_" + groupsArray[i].toUpperCase();
    }
    return groupsArray;
  }

  private String[] appendRolePrefix(String allowedGroups, String adminGroups) {
    String[] allowedGroupArray = appendRolePrefix(allowedGroups);
    String[] adminGroupArray = appendRolePrefix(adminGroups);
    return (String[]) ArrayUtils.addAll(allowedGroupArray, adminGroupArray);
  }

}