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
package org.schedoscope.metascope.service;

import org.schedoscope.metascope.model.MetascopeActivity;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class MetascopeActivityService {

    @Autowired
    private MetascopeActivityRepository activityEntityRepository;

    public List<MetascopeActivity> getActivities() {
        return activityEntityRepository.findFirst10ByOrderByTimestampDesc();
    }

    @Transactional
    public void createUpdateDocumentActivity(MetascopeTable table, String username) {
        createAndSave(MetascopeActivity.ActivityType.UPDATE_DOCUMENTATION, table, username);
    }

    @Transactional
    public void createNewCommentActivity(MetascopeTable tableEntity, String username) {
        createAndSave(MetascopeActivity.ActivityType.NEW_COMMENT, tableEntity, username);
    }

    @Transactional
    public void createUpdateTaxonomyActivity(MetascopeTable tableEntity, String username) {
        createAndSave(MetascopeActivity.ActivityType.UPDATE_TAGS, tableEntity, username);
    }

    @Transactional
    public void createUpdateTableMetadataActivity(MetascopeTable tableEntity, String username) {
        createAndSave(MetascopeActivity.ActivityType.UPDATE_TABLE_METADATA, tableEntity, username);
    }

    private void createAndSave(MetascopeActivity.ActivityType activityType, MetascopeTable table, String username) {
        String activityKey = table.getFqdn() + "." + username + "." + activityType.toString();
        MetascopeActivity activityEntity = activityEntityRepository.findOne(activityKey);
        if (activityEntity == null) {
            activityEntity = new MetascopeActivity();
            activityEntity.setActivityId(activityKey);
            activityEntity.setType(activityType);
            activityEntity.setUsername(username);
            activityEntity.setTable(table);
        }
        activityEntity.setTimestamp(System.currentTimeMillis());
        activityEntityRepository.save(activityEntity);
    }

    public void setActivityEntityRepository(MetascopeActivityRepository activityEntityRepository) {
        this.activityEntityRepository = activityEntityRepository;
    }

}
