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

import static org.junit.Assert.assertEquals;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.ActivityEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.model.ActivityEntity.ActivityType;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ActivityEntityServiceTest extends SpringTest {

  @Test
  public void activityService_01_createUpdateDocumentActivity_create() {
    activityEntityRepository.deleteAll();

    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 0);

    activityEntityService.createUpdateDocumentActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_DOCUMENTATION);
  }

  @Test
  public void activityService_02_createUpdateDocumentActivity_update() {
    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 1);

    activityEntityService.createUpdateDocumentActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_DOCUMENTATION);
  }

  @Test
  public void activityService_03_createNewCommentActivity_create() {
    activityEntityRepository.deleteAll();

    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 0);

    activityEntityService.createNewCommentActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.NEW_COMMENT);
  }

  @Test
  public void activityService_04_createNewCommentActivity_update() {
    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 1);

    activityEntityService.createNewCommentActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.NEW_COMMENT);
  }

  @Test
  public void activityService_05_createUpdateTaxonomyActivity_create() {
    activityEntityRepository.deleteAll();

    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 0);

    activityEntityService.createUpdateTaxonomyActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_TAGS);
  }

  @Test
  public void activityService_06_createUpdateTaxonomyActivity_update() {
    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 1);

    activityEntityService.createUpdateTaxonomyActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_TAGS);
  }

  @Test
  public void activityService_07_createUpdateTableMetadataActivity_create() {
    activityEntityRepository.deleteAll();

    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 0);

    activityEntityService.createUpdateTableMetadataActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_TABLE_METADATA);
  }

  @Test
  public void activityService_08_createUpdateTableMetadataActivity_update() {
    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getLoggedInUser();

    assertEquals(size(activityEntityRepository.findAll()), 1);

    activityEntityService.createUpdateTableMetadataActivity(tableEntity, userEntity);

    Iterable<ActivityEntity> allActivites = activityEntityRepository.findAll();
    assertEquals(size(allActivites), 1);

    ActivityEntity activityEntity = allActivites.iterator().next();
    assertEquals(activityEntity.getUser().getUsername(), userEntity.getUsername());
    assertEquals(activityEntity.getTable().getFqdn(), tableEntity.getFqdn());
    assertEquals(activityEntity.getType(), ActivityType.UPDATE_TABLE_METADATA);
  }

}
