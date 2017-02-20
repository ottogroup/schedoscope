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
package org.schedoscope.metascope.task;

import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.util.TaskMutex;
import org.schedoscope.metascope.util.model.SchedoscopeInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MetascopeTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeTask.class);

  @Autowired
  private MetascopeConfig config;

  @Autowired
  private SchedoscopeTask syncTask;

  @Autowired
  private MetastoreTask metastoreSyncTask;

  @Autowired
  private TaskMutex taskMutex;

  @Override
  @Transactional
  public void run() {
    long ts = System.currentTimeMillis();
    if (!taskMutex.isSchedoscopeTaskRunning()) {
      taskMutex.setSchedoscopeTaskRunning(true);
      for (SchedoscopeInstance schedoscopeInstance : config.getSchedoscopeInstances()) {
        syncTask.forInstance(schedoscopeInstance).run(ts);
      }
      metastoreSyncTask.run(ts);
      taskMutex.setSchedoscopeTaskRunning(false);
    }
  }

}
