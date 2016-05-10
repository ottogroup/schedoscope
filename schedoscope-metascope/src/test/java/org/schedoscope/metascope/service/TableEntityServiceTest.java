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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.model.CategoryObjectEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.service.JobSchedulerService;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Iterators;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TableEntityServiceTest extends SpringTest {

  private static final String TIMESTAMP_FIELD = "occurred_at";
  private static final String TIMESTAMP_FIELD_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  private static final String EMPTY_STRING = "";

  @Before
  public void setupLocal() {
    JobSchedulerService jobSchedulerService = mock(JobSchedulerService.class);
    mockField(tableEntityService, "jobSchedulerService", jobSchedulerService);
  }

  @Test
  public void tableService_01_setPersonResponsibleTableNotExists() {
    int nrOfActivitesBefore = Iterators.size(activityEntityRepository.findAll().iterator());

    tableEntityService.setPersonResponsible(NON_EXIST_TABLE, getTestUser().getUsername());

    int nrOfActivitesAfter = Iterators.size(activityEntityRepository.findAll().iterator());

    assertEquals(nrOfActivitesBefore, nrOfActivitesAfter);
  }

  @Test
  public void tableService_02_setPersonResponsibleUserNotExists() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getPersonResponsible(), null);

    tableEntityService.setPersonResponsible(tableEntity.getFqdn(), NON_EXIST_USER_FULLNAME);

    tableEntity = getTestTable();

    assertEquals(tableEntity.getPersonResponsible(), NON_EXIST_USER_FULLNAME);
  }

  @Test
  public void tableService_03_setPersonResponsible() {
    TableEntity tableEntity = getTestTable();
    UserEntity userEntity = getTestUser();

    assertEquals(tableEntity.getPersonResponsible(), NON_EXIST_USER_FULLNAME);

    tableEntityService.setPersonResponsible(tableEntity.getFqdn(), userEntity.getFullname());

    tableEntity = getTestTable();

    UserEntity respPerson = userEntityService.findByFullname(userEntity.getFullname());
    assertEquals(tableEntity.getPersonResponsible(), userEntity.getFullname());
    assertEquals(respPerson.getUsername(), userEntity.getUsername());
  }

  @Test
  public void tableService_04_setTimestampFieldFieldNotExist() {
    TableEntity tableEntity = getTestTable();

    String currentTimeStampField = tableEntity.getTimestampField();
    String currentTimestampFieldFormat = tableEntity.getTimestampFieldFormat();

    tableEntityService.setTimestampField(tableEntity.getFqdn(), EMPTY_STRING, EMPTY_STRING);

    tableEntity = getTestTable();

    assertEquals(tableEntity.getTimestampField(), currentTimeStampField);
    assertEquals(tableEntity.getTimestampFieldFormat(), currentTimestampFieldFormat);
  }

  @Test
  public void tableService_05_setTimestampField() {
    TableEntity tableEntity = getTestTable();

    tableEntityService.setTimestampField(tableEntity.getFqdn(), TIMESTAMP_FIELD, TIMESTAMP_FIELD_FORMAT);

    tableEntity = getTestTable();

    assertEquals(tableEntity.getTimestampField(), TIMESTAMP_FIELD);
    assertEquals(tableEntity.getTimestampFieldFormat(), TIMESTAMP_FIELD_FORMAT);
  }

  @Test
  @Transactional
  public void tableService_06_addFavourite() {
    TableEntity tableEntity = getTestTable();

    tableEntityService.addOrRemoveFavourite(tableEntity.getFqdn());

    UserEntity userEntity = getLoggedInUser();

    assertEquals(userEntity.getFavourites().size(), 1);
    assertEquals(userEntity.getFavourites().get(0), tableEntity.getFqdn());
  }

  @Test
  @Transactional
  public void tableService_07_removeFavourite() {
    TableEntity tableEntity = getTestTable();

    tableEntityService.addOrRemoveFavourite(tableEntity.getFqdn());

    UserEntity userEntity = getLoggedInUser();

    assertEquals(userEntity.getFavourites().size(), 1);
    assertEquals(userEntity.getFavourites().get(0), tableEntity.getFqdn());
  }

  @Test
  public void tableService_08_increaseViewCount() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getViewCount(), 0);

    tableEntityService.increaseViewCount(tableEntity.getFqdn());

    tableEntity = getTestTable();
    assertEquals(tableEntity.getViewCount(), 1);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void tableService_09_addCategoryObject() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getCategoryObjects().size(), 0);
    assertEquals(tableEntity.getCategoryNames().size(), 0);
    
    Iterable<CategoryObjectEntity> coEntites = coEntityRepository.findAll();

    Map<String, String[]> parameterMap = new HashMap<String, String[]>();
    parameterMap.put("SomeCategoryObjects", new String[]{"" + coEntites.iterator().next().getCategoryObjectId()});
    tableEntityService.setCategoryObjects(tableEntity.getFqdn(), parameterMap);

    tableEntity = getTestTable();
    assertEquals(tableEntity.getCategoryNames().size(), 1);
    assertEquals(tableEntity.getCategoryNames().get(0), TEST_CATEGORY_NAME);
    assertEquals(tableEntity.getCategoryObjects().size(), 1);
    assertEquals(tableEntity.getCategoryObjects().get(0).getName(), TEST_CATEGORY_OBJECT_NAME);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void tableService_10_removeCategoryObject() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getCategoryObjects().size(), 1);
    assertEquals(tableEntity.getCategoryNames().size(), 1);

    tableEntityService.setCategoryObjects(tableEntity.getFqdn(), null);

    tableEntity = getTestTable();
    assertEquals(tableEntity.getCategoryNames().size(), 0);
    assertEquals(tableEntity.getCategoryObjects().size(), 0);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void tableService_11_addTag() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getTags().size(), 0);

    tableEntityService.setTags(tableEntity.getFqdn(), TEST_TAG);

    tableEntity = getTestTable();
    assertEquals(tableEntity.getTags().size(), 1);
    assertEquals(tableEntity.getTags().get(0), TEST_TAG);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void tableService_12_removeTag() {
    TableEntity tableEntity = getTestTable();

    assertEquals(tableEntity.getTags().size(), 1);

    tableEntityService.setTags(tableEntity.getFqdn(), null);

    tableEntity = getTestTable();
    assertEquals(tableEntity.getTags().size(), 0);
  }

  @Test
  @Transactional
  public void tableService_13_getParameterValues() {
    TableEntity tableEntity = getTestTable();

    Map<String, List<String>> parameterValues = tableEntityService.getParameterValues(tableEntity);

    assertTrue(parameterValues != null);

    Set<Entry<String, List<String>>> entrySet = parameterValues.entrySet();
    assertEquals(entrySet.size(), 3);

    int count = 0;
    for (Entry<String, List<String>> entry : entrySet) {
      count += entry.getValue().size();
    }
    assertEquals(count, 44);
  }

  @Test
  @Transactional
  public void tableService_14_getRandomParameterValueForExistingParameter() {
    TableEntity tableEntity = getTestTable();
    FieldEntity existingYearParameter = tableEntity.getParameters().get(0);

    String parameterValue = tableEntityService.getRandomParameterValue(tableEntity, existingYearParameter);

    assertTrue(parameterValue != null);
    assertTrue(!parameterValue.isEmpty());
  }

  @Test
  @Transactional
  public void tableService_15_getRandomParameterValueForNonExistingParameter() {
    TableEntity tableEntity = getTestTable();
    FieldEntity nonExistingParameter = getNonExistParameter();

    String parameterValue = tableEntityService.getRandomParameterValue(tableEntity, nonExistingParameter);

    assertTrue(parameterValue == null);
  }

  @Test
  @Transactional
  public void tableService_16_getTransitiveDependencies() {
    TableEntity tableEntity = getTestTable();

    List<TableDependencyEntity> tableDependencies = tableEntityService.getTransitiveDependencies(tableEntity);

    assertTrue(tableDependencies != null);
    assertEquals(tableDependencies.size(), 3);
  }

  @Test
  @Transactional
  public void tableService_17_getTransitiveSuccessors() {
    TableEntity tableEntity = getTestTable();

    List<TableDependencyEntity> tableSuccessors = tableEntityService.getTransitiveSuccessors(tableEntity);

    assertTrue(tableSuccessors != null);
    assertEquals(tableSuccessors.size(), 4);
  }

}
