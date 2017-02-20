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
package org.schedoscope.metascope;

import com.typesafe.config.ConfigFactory;
import org.schedoscope.conf.BaseSettings;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Metascope {

  private static final Logger LOG = LoggerFactory.getLogger(Metascope.class);

  private ConfigurableApplicationContext ctx;

  public static void main(String[] args) {
    new Metascope().start(args);
  }

  public void start(String[] args) {
    /* set some mandatory configs before application start */
    MetascopeConfig config = new MetascopeConfig(new BaseSettings(ConfigFactory.load()));
    System.setProperty("server.port", String.valueOf(config.getPort()));
    System.setProperty("spring.jpa.database-platform", config.getRepositoryDialect());
    System.setProperty("logging.level.org.schedoscope", config.getLogLevel());
    System.setProperty("logging.file", config.getLogfilePath());
    System.setProperty("spring.profiles.active", "production");

    /* start metascope spring boot application */
    this.ctx = SpringApplication.run(Metascope.class, args);
    LOG.info("Metascope webservice started.");
  }

  public void stop() {
    ctx.stop();
  }

}