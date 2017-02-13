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
package org.schedoscope.metascope.repository;

import org.junit.runner.RunWith;
import org.schedoscope.metascope.model.MetascopeActivity;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeActivityRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeActivityRepository metascopeActivityRepository;

  @Before
  public void setup() {
    long ts = System.currentTimeMillis();

    for (int i = 0; i < 20; i++) {
      MetascopeActivity metascopeActivity = new MetascopeActivity();
      metascopeActivity.setActivityId(String.valueOf(i));
      metascopeActivity.setTimestamp(ts - i);
      this.entityManager.persist(metascopeActivity);
    }
  }

  @Test
  public void findFirst10ByOrderByTimestampDescTest() {
    List<MetascopeActivity> activities = metascopeActivityRepository.findFirst10ByOrderByTimestampDesc();

    assertNotNull(activities);
    assertTrue(activities.size() == 10);

    for (MetascopeActivity activity : activities) {
      assertTrue(Integer.valueOf(activity.getActivityId()) <= 10);
    }

  }

}
