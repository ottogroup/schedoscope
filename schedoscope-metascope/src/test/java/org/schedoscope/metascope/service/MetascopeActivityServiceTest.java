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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.schedoscope.metascope.model.MetascopeActivity;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeActivityRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class MetascopeActivityServiceTest {

    /* service to test */
    private MetascopeActivityService metascopeActivityService;

    /* mocked fields */
    private MetascopeActivityRepository metascopeActivityRepositoryMock;

    /* database stubs */
    private Set<MetascopeActivity> activityDatabaseStub;

    /* test data */
    private MetascopeActivity metascopeActivity;
    private MetascopeTable metascopeTable;

    @Before
    public void setup() {
        this.metascopeActivityService = new MetascopeActivityService();
        this.metascopeActivityRepositoryMock = mock(MetascopeActivityRepository.class);
        this.activityDatabaseStub = new HashSet<>();
        metascopeActivityService.setActivityEntityRepository(metascopeActivityRepositoryMock);

        this.metascopeActivity = new MetascopeActivity();
        this.metascopeTable = new MetascopeTable();
        metascopeTable.setFqdn("testTable");
        metascopeActivity.setActivityId("update-documentation");
        metascopeActivity.setTable(metascopeTable);
        metascopeActivity.setType(MetascopeActivity.ActivityType.UPDATE_DOCUMENTATION);
        metascopeActivity.setUsername("test");

        when(metascopeActivityRepositoryMock.save(any(MetascopeActivity.class))).then(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                activityDatabaseStub.add(metascopeActivity);
                return null;
            }
        });
    }

    @Test
    public void createUpdateDocumentActivityTest() {
    /* pre assertions */
        assertEquals(0, activityDatabaseStub.size());

        metascopeActivityService.createUpdateDocumentActivity(metascopeTable, "testUser");

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals(metascopeActivity, activityDatabaseStub.iterator().next());
    }

    @Test
    public void createNewCommentActivityTest() {
    /* pre assertions */
        assertEquals(0, activityDatabaseStub.size());

        metascopeActivityService.createNewCommentActivity(metascopeTable, "testUser");

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals(metascopeActivity, activityDatabaseStub.iterator().next());
    }

    @Test
    public void createUpdateTaxonomyActivityTest() {
    /* pre assertions */
        assertEquals(0, activityDatabaseStub.size());

        metascopeActivityService.createUpdateTaxonomyActivity(metascopeTable, "testUser");

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals(metascopeActivity, activityDatabaseStub.iterator().next());
    }

    @Test
    public void createUpdateTableMetadataActivityTest() {
    /* pre assertions */
        assertEquals(0, activityDatabaseStub.size());

        metascopeActivityService.createUpdateTableMetadataActivity(metascopeTable, "testUser");

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals(metascopeActivity, activityDatabaseStub.iterator().next());
    }

    @Test
    public void getActivitiesTest() {
        List<MetascopeActivity> mockedActivities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MetascopeActivity metascopeActivity = new MetascopeActivity();
            metascopeActivity.setActivityId(String.valueOf(i));
            metascopeActivity.setTable(null);
            metascopeActivity.setTimestamp(System.currentTimeMillis());
            metascopeActivity.setType(MetascopeActivity.ActivityType.NEW_COMMENT);
            metascopeActivity.setUsername("test");
            mockedActivities.add(metascopeActivity);
        }
        when(metascopeActivityRepositoryMock.findFirst10ByOrderByTimestampDesc()).thenReturn(mockedActivities);

        List<MetascopeActivity> activities = metascopeActivityService.getActivities();

        assertNotNull(activities);
        assertEquals(10, activities.size());
    }

}
