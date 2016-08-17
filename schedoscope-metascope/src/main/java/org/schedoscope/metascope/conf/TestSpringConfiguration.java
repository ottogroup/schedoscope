/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.metascope.conf;

import com.typesafe.config.ConfigFactory;
import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.tasks.repository.jdbc.NativeSqlRepository;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.embedded.ServletListenerRegistrationBean;
import org.springframework.context.annotation.*;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.web.session.HttpSessionEventPublisher;

import javax.sql.DataSource;

/**
 * Configuration to secure the application (login)
 */
@Profile("test")
@Configuration
@ComponentScan(basePackages = {"org.schedoscope.metascope"}, excludeFilters = {@Filter(type = FilterType.ASSIGNABLE_TYPE, value = {ProductionSpringConfiguration.class})})
@EnableAsync
public class TestSpringConfiguration extends WebSecurityConfigurerAdapter {

    @Bean
    public LdapTemplate ldapTemplate() {
        return new LdapTemplate(new LdapContextSource());
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
        return DataSourceBuilder.create().username("sa").password("").url("jdbc:derby:memory:metascope;create=true")
                .build();
    }

    @Bean
    public RepositoryDAO repositoryDAO() {
        return new NativeSqlRepository();
    }

    @Bean
    public SolrFacade solrFacade() {
        return new SolrFacade("target/test-resources/solr");
    }

}