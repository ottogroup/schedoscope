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
package org.schedoscope.metascope.service;

import java.util.List;

import org.schedoscope.metascope.model.ActivityEntity;
import org.schedoscope.metascope.model.ActivityEntity.ActivityType;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.key.ActivityEntityKey;
import org.schedoscope.metascope.repository.ActivityEntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ActivityEntityService {

	@Autowired
	private ActivityEntityRepository activityEntityRepository;

	public List<ActivityEntity> getActivities() {
		return activityEntityRepository.findFirst10ByOrderByTimestampDesc();
	}

	@Transactional
	public void createUpdateDocumentActivity(TableEntity tableEntity,
			String username) {
		createAndSave(ActivityType.UPDATE_DOCUMENTATION, tableEntity, username);
	}

	@Transactional
	public void createNewCommentActivity(TableEntity tableEntity,
			String username) {
		createAndSave(ActivityType.NEW_COMMENT, tableEntity, username);
	}

	@Transactional
	public void createUpdateTaxonomyActivity(TableEntity tableEntity,
			String username) {
		createAndSave(ActivityType.UPDATE_TAGS, tableEntity, username);
	}

	@Transactional
	public void createUpdateTableMetadataActivity(TableEntity tableEntity,
			String username) {
		createAndSave(ActivityType.UPDATE_TABLE_METADATA, tableEntity, username);
	}

	private void createAndSave(ActivityType activityType,
			TableEntity tableEntity, String username) {
		ActivityEntityKey key = new ActivityEntityKey(activityType, username,
				tableEntity);
		ActivityEntity activityEntity = activityEntityRepository.findOne(key);
		if (activityEntity == null) {
			activityEntity = new ActivityEntity();
			activityEntity.setType(activityType);
			activityEntity.setUsername(username);
			activityEntity.setTable(tableEntity);
		}
		activityEntity.setTimestamp(System.currentTimeMillis());
		activityEntityRepository.save(activityEntity);
	}

}
