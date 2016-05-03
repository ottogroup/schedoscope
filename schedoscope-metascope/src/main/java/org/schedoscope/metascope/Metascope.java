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
package org.schedoscope.metascope;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.tasks.MetascopeTask;
import org.schedoscope.metascope.tasks.SchedoscopeStatusTask;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.typesafe.config.ConfigFactory;

@SpringBootApplication
public class Metascope {

  private static final Logger LOG = LoggerFactory.getLogger(Metascope.class);

  private ConfigurableApplicationContext applicationContext;
  private ScheduledThreadPoolExecutor executor;

	public void start(String[] args) {
    /* set some mandatory configs before application start */
    MetascopeConfig config = new MetascopeConfig(new BaseSettings(ConfigFactory.load()));
    System.setProperty("server.port", String.valueOf(config.getPort()));
    System.setProperty("spring.jpa.database-platform", config.getRepositoryDialect());
    System.setProperty("logging.level.org.schedoscope", config.getLogLevel());
    System.setProperty("logging.file", config.getLogfilePath());
    System.setProperty("spring.profiles.active", "production");

    /* start metascope spring boot application */
    this.applicationContext = SpringApplication.run(Metascope.class, args);

    SolrFacade solr = applicationContext.getBean(SolrFacade.class);
    RepositoryDAO repo = applicationContext.getBean(RepositoryDAO.class);
    DataSource dataSource = applicationContext.getBean(DataSource.class);
    SchedoscopeUtil schedoscopeUtil = applicationContext.getBean(SchedoscopeUtil.class);

    /* start metascope task */
    MetascopeTask metascopeTask = new MetascopeTask(repo, dataSource, solr, config, schedoscopeUtil);
    SchedoscopeStatusTask statusTask = new SchedoscopeStatusTask(repo, dataSource, solr, schedoscopeUtil);

    this.executor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture<?> metascopeTaskFuture = executor.schedule(metascopeTask, 5, TimeUnit.SECONDS);
    ScheduledFuture<?> statusTaskFuture = executor.scheduleAtFixedRate(statusTask, 5, 5, TimeUnit.SECONDS);

    /* MetascopeTask schedules itself dynamically */
    metascopeTask.setExecutor(executor);

    try {
      metascopeTaskFuture.get();
      statusTaskFuture.get();
    } catch (Throwable t) {
      LOG.error("Exception in future tasks", t);
    }
  }

	public void stop() throws InterruptedException {
		 this.applicationContext.stop();
		 this.executor.awaitTermination(5, TimeUnit.SECONDS);
  }
	
  public static void main(String[] args) {
  	new Metascope().start(args);
  }

}