/**
 * Copyright 2017 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.config;

import com.typesafe.config.ConfigFactory;
import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.index.SolrFacade;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.*;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.session.HttpSessionEventPublisher;

import javax.sql.DataSource;

@Profile("test")
@Configuration
@ComponentScan(basePackages = {"org.schedoscope.metascope"}, excludeFilters = {@Filter(type = FilterType.ASSIGNABLE_TYPE, value = {ProductionSpringConfiguration.class})})
@EnableAsync
public class TestSpringConfiguration extends WebSecurityConfigurerAdapter {

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.authorizeRequests().anyRequest().permitAll();
    http.csrf().disable();
  }

  @Bean
  public LdapTemplate ldapTemplate() {
    return new LdapTemplate(new LdapContextSource());
  }

  @Bean
  public SessionRegistry sessionRegistry() {
    return new SessionRegistryImpl();
  }

  @Bean
  public org.springframework.boot.web.servlet.ServletListenerRegistrationBean<HttpSessionEventPublisher> httpSessionEventPublisher() {
    return new org.springframework.boot.web.servlet.ServletListenerRegistrationBean<HttpSessionEventPublisher>(new HttpSessionEventPublisher());
  }

  @Bean
  public MetascopeConfig metascopeConfig() {
    return new MetascopeConfig(new BaseSettings(ConfigFactory.load()));
  }

  @Bean
  public DataSource dataSource() {
    return DataSourceBuilder.create().username("sa").password("sa").url("jdbc:h2:mem:metascope")
      .build();
  }

  @Bean
  public SolrFacade solrFacade() {
    return new SolrFacade();
  }

}