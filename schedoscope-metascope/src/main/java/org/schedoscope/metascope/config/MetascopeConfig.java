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

import com.typesafe.config.ConfigValue;
import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.util.model.SchedoscopeInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class MetascopeConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeConfig.class);

  private static final String METASCOPE_JAR_LOCATION = "{metascope.dir}";

  private String classLocations;

  private int port;

  /* Schedoscope settings */
  private List<SchedoscopeInstance> schedoscopeInstances;

  /* Authentication settings */
  private String authenticationMethod;
  private String ldapUrl;
  private String managerDn;
  private String managerPassword;
  private String userDnPattern;
  private String groupSearchBase;
  private String allowedGroups;
  private String adminGroups;

  /* Hadoop settings */
  private String hdfs;

  /* Kerberos settings */
  private String kerberosPrincipal;

  /* Metastore settings */
  private String metastoreThriftUri;

  /* Hive settings */
  private String hiveJdbcDriver;
  private String hiveServerUrl;

  /* Repository settings */
  private String repositoryUrl;
  private String repositoryUser;
  private String repositoryPassword;
  private String repositoryDialect;

  /* Solr settings */
  private String solrUrl;

  /* Logging settings */
  private String logfilePath;
  private String logLevel;

  public MetascopeConfig(BaseSettings config) {
    String location = getExecutionLocation();
    this.classLocations = location.substring(0, location.lastIndexOf("/"));

    this.port = config.metascopePort();

    this.schedoscopeInstances = new ArrayList<>();

    if (config.metascopeSchedoscopeInstances() != null) {
      for (ConfigValue val : config.metascopeSchedoscopeInstances()) {
        Map<String, Object> props = (Map<String, Object>) val.unwrapped();
        schedoscopeInstances.add(new SchedoscopeInstance(String.valueOf(props.get("id")),
          String.valueOf(props.get("host")), (int) props.get("port")));
      }
    }

    this.authenticationMethod = getString(config.metascopeAuthMethod());
    this.ldapUrl = getString(config.metascopeLdapUrl());
    this.managerDn = getString(config.metascopeLdapManagerDn());
    this.managerPassword = getString(config.metascopeLdapManagerPw());
    this.userDnPattern = getString(config.metascopeLdapUserDn());
    this.groupSearchBase = getString(config.metascopeLdapGroupSearchBase());
    this.allowedGroups = getString(config.metascopeLdapAllowedGroups());
    this.adminGroups = getString(config.metascopeLdapAdminGroups());

    this.hdfs = getString(config.hdfs());

    this.kerberosPrincipal = getString(config.kerberosPrincipal());

    this.metastoreThriftUri = getString(config.metastoreUri());

    this.hiveJdbcDriver = getString("org.apache.hive.jdbc.HiveDriver");
    this.hiveServerUrl = getString(config.jdbcUrl());

    this.repositoryUrl = getString(config.metascopeRepositoryUrl());
    this.repositoryUser = getString(config.metascopeRepositoryUser());
    this.repositoryPassword = getString(config.metascopeRepositoryPw());
    this.repositoryDialect = getString(config.metascopeRepositoryDialect());

    this.solrUrl = getString(config.metascopeSolrUrl());

    this.logfilePath = getString(config.metascopeLoggingFile());
    this.logLevel = getString(config.metascopeLoggingLevel());
  }

  private String getString(String value) {
    if (value == null) {
      return null;
    }
    return value.replace(METASCOPE_JAR_LOCATION, classLocations).trim();
  }

  public int getPort() {
    return port;
  }

  public List<SchedoscopeInstance> getSchedoscopeInstances() {
    return schedoscopeInstances;
  }

  public String getAuthenticationMethod() {
    return authenticationMethod;
  }

  public String getLdapUrl() {
    return ldapUrl;
  }

  public String getManagerDn() {
    return managerDn;
  }

  public String getManagerPassword() {
    return managerPassword;
  }

  public String getUserDnPattern() {
    return userDnPattern;
  }

  public String getGroupSearchBase() {
    return groupSearchBase;
  }

  public String getAllowedGroups() {
    return allowedGroups;
  }

  public String getAdminGroups() {
    return adminGroups;
  }

  public String getHdfs() {
    return hdfs;
  }

  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public String getRepositoryUrl() {
    return repositoryUrl;
  }

  public String getRepositoryUser() {
    return repositoryUser;
  }

  public String getRepositoryPassword() {
    return repositoryPassword;
  }

  public String getRepositoryDialect() {
    return repositoryDialect;
  }

  public String getLogfilePath() {
    return logfilePath;
  }

  public String getMetastoreThriftUri() {
    return metastoreThriftUri;
  }

  public String getHiveJdbcDriver() {
    return hiveJdbcDriver;
  }

  public String getHiveServerUrl() {
    return hiveServerUrl;
  }

  public String getSolrUrl() {
    return solrUrl;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public boolean withUserManagement() {
    return !getAuthenticationMethod().equalsIgnoreCase("ldap");
  }

  private String getExecutionLocation() {
    String execLocation = null;
    try {
      execLocation = MetascopeConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
    } catch (URISyntaxException e) {
      LOG.warn("Could not get path of metascope.jar; '" + METASCOPE_JAR_LOCATION
              + "' is set to current working directory.");
      execLocation = ".";
    }

    // TODO hotfix to determine folder to create solr index and repository.
    // path should be determined in another dynamic way
    if (execLocation.endsWith("/target/classes/")) {
      // executed from classes e.g. eclipse, mvn exec:java, ...
      execLocation += "../metascope-deployment/";
    }

    return execLocation;
  }

  public SchedoscopeInstance getSchedoscopeInstanceById(String schedoscopeId) {
    for (SchedoscopeInstance schedoscopeInstance : schedoscopeInstances) {
      if (schedoscopeInstance.getId().equals(schedoscopeId)) {
        return schedoscopeInstance;
      }
    }
    return null;
  }

}
