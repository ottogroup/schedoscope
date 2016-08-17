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
package org.schedoscope.metascope.service;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.jobs.DataDistributionJob;
import org.schedoscope.metascope.jobs.RowCountJob;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.JobMetadataEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.model.key.JobMetadataEntityKey;
import org.schedoscope.metascope.repository.DataDistributionEntityRepository;
import org.schedoscope.metascope.repository.JobMetadataEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.schedoscope.metascope.repository.ViewEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DataDistributionService {

    private static final Logger LOG = LoggerFactory.getLogger(DataDistributionService.class);

    @Autowired
    private TableEntityRepository tableEntityRepository;
    @Autowired
    private ViewEntityRepository viewEntityRepository;
    @Autowired
    private DataDistributionEntityRepository dataDistributionEntityRepository;
    @Autowired
    private JobMetadataEntityRepository jobMetadataEntityRepository;
    @Autowired
    @Qualifier("taskExecutor")
    private TaskExecutor taskExecutor;
    @Autowired
    private MetascopeConfig config;

    @Transactional
    public boolean calculateDistributionForView(String urlPath) {
        ViewEntity viewEntity = viewEntityRepository.findByUrlPath(urlPath);

        if (viewEntity != null) {
            TableEntity tableEntity = tableEntityRepository.findByFqdn(viewEntity.getFqdn());
            if (tableEntity != null) {
                LOG.info("Scheduling RowCountJob for partition '{}'", viewEntity.getFqdn());
                String rowJobMetadataKey = viewEntity.getUrlPath();
                String rowJobMetadataField = "rowcount";
                setJobMetadata(rowJobMetadataKey, rowJobMetadataField);
                taskExecutor.execute(new RowCountJob(viewEntity, viewEntityRepository, jobMetadataEntityRepository, config,
                        rowJobMetadataKey, rowJobMetadataField));

                for (FieldEntity fieldEntity : tableEntity.getFields()) {
                    LOG.info("Scheduling DataDistributionJob for partition '{}' and field '{}'", viewEntity.getFqdn(),
                            fieldEntity.getName());
                    String jobMetadataKey = viewEntity.getUrlPath();
                    String jobMetadataField = fieldEntity.getName();
                    setJobMetadata(jobMetadataKey, jobMetadataField);
                    taskExecutor.execute(new DataDistributionJob(tableEntity, viewEntity, fieldEntity,
                            dataDistributionEntityRepository, jobMetadataEntityRepository, config, jobMetadataKey, jobMetadataField));
                }
            }
            return true;
        }
        return false;
    }

    private void setJobMetadata(String jobMetadataKey, String jobMetadataField) {
        JobMetadataEntity jobMetadata = jobMetadataEntityRepository.findOne(new JobMetadataEntityKey(jobMetadataKey,
                jobMetadataField));
        if (jobMetadata == null) {
            jobMetadata = new JobMetadataEntity(jobMetadataKey, jobMetadataField, false);
        } else {
            jobMetadata.setFinished(false);
        }
        jobMetadataEntityRepository.save(jobMetadata);
    }

}
