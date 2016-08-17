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
package org.schedoscope.metascope.jobs;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.*;
import org.schedoscope.metascope.model.key.JobMetadataEntityKey;
import org.schedoscope.metascope.repository.DataDistributionEntityRepository;
import org.schedoscope.metascope.repository.JobMetadataEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.schedoscope.metascope.repository.ViewEntityRepository;
import org.schedoscope.metascope.util.HiveServerConnection;

public abstract class AsyncRepositoryJob implements Runnable {

    protected TableEntity tableEntity;
    protected ViewEntity viewEntity;
    protected FieldEntity fieldEntity;
    protected TableEntityRepository tableEntityRepository;
    protected ViewEntityRepository viewEntityRepository;
    protected DataDistributionEntityRepository dataDistributionEntityRepository;
    protected JobMetadataEntityRepository jobMetadataEntityRepository;
    protected HiveServerConnection connection;
    protected String jobMetadataKey;
    protected String jobMetadataField;

    public AsyncRepositoryJob(ViewEntity viewEntity, ViewEntityRepository viewEntityRepository,
                              JobMetadataEntityRepository jobMetadataEntityRepository, MetascopeConfig config, String jobMetadataKey,
                              String jobMetadataField) {
        this.viewEntity = viewEntity;
        this.viewEntityRepository = viewEntityRepository;
        this.jobMetadataEntityRepository = jobMetadataEntityRepository;
        this.connection = new HiveServerConnection(config);
        this.jobMetadataKey = jobMetadataKey;
        this.jobMetadataField = jobMetadataField;
    }

    public AsyncRepositoryJob(TableEntity tableEntity, ViewEntity viewEntity, FieldEntity fieldEntity,
                              DataDistributionEntityRepository dataDistributionEntityRepository,
                              JobMetadataEntityRepository jobMetadataEntityRepository, MetascopeConfig config, String jobMetadataKey,
                              String jobMetadataField) {
        this.tableEntity = tableEntity;
        this.viewEntity = viewEntity;
        this.fieldEntity = fieldEntity;
        this.dataDistributionEntityRepository = dataDistributionEntityRepository;
        this.jobMetadataEntityRepository = jobMetadataEntityRepository;
        this.connection = new HiveServerConnection(config);
        this.jobMetadataKey = jobMetadataKey;
        this.jobMetadataField = jobMetadataField;
    }

    public AsyncRepositoryJob(TableEntity tableEntity, TableEntityRepository tableEntityRepository,
                              JobMetadataEntityRepository jobMetadataEntityRepository, MetascopeConfig config, String jobMetadataKey,
                              String jobMetadataField) {
        this.tableEntity = tableEntity;
        this.tableEntityRepository = tableEntityRepository;
        this.jobMetadataEntityRepository = jobMetadataEntityRepository;
        this.connection = new HiveServerConnection(config);
        this.jobMetadataKey = jobMetadataKey;
        this.jobMetadataField = jobMetadataField;
    }

    protected abstract void execute();

    @Override
    public void run() {
        preJob();
        execute();
        postJob();
    }

    private void preJob() {
    /* connect to hiveserver2 */
        connection.connect();
    }

    private void postJob() {
        JobMetadataEntity jobMetadata = jobMetadataEntityRepository.findOne(new JobMetadataEntityKey(jobMetadataKey,
                jobMetadataField));
        if (jobMetadata == null) {
            return;
        }

        jobMetadata.setFinished(true);
        jobMetadataEntityRepository.save(jobMetadata);

    /* close connection to hiveserver2 */
        connection.close();
    }

    protected String addParameters(String sql) {
        if (!viewEntity.getParameters().isEmpty()) {
            sql += " where ";
            String whereCond = "";
            for (ParameterValueEntity kve : viewEntity.getParameters()) {
                if (!whereCond.isEmpty()) {
                    whereCond += " and ";
                }
                whereCond += kve.getKey() + " = '" + kve.getValue() + "'";
            }
            sql += whereCond;
        }
        return sql;
    }

}
