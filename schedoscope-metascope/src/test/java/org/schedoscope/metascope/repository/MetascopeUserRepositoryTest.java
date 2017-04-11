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
package org.schedoscope.metascope.repository;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.schedoscope.metascope.model.MetascopeUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeUserRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeUserRepository metascopeUserRepository;

  private MetascopeUser metascopeUser;

  @Before
  public void setup() {
    MetascopeUser tmpMetascopeUser = new MetascopeUser();
    tmpMetascopeUser.setUsername("testUser");
    tmpMetascopeUser.setEmail("test@ottogroup.com");
    tmpMetascopeUser.setFullname("Foo Bar");
    this.metascopeUser = this.entityManager.persist(tmpMetascopeUser);
  }

  @Test
  public void findByUserNameTest() {
    MetascopeUser user = metascopeUserRepository.findByUsername("testUser");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

  @Test
  public void findByEmailTest() {
    MetascopeUser user = metascopeUserRepository.findByEmail("test@ottogroup.com");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

  @Test
  public void findByFullnameTest() {
    MetascopeUser user = metascopeUserRepository.findByFullname("Foo Bar");

    assertNotNull(user);
    assertEquals("testUser", user.getUsername());
    assertEquals("test@ottogroup.com", user.getEmail());
    assertEquals("Foo Bar", user.getFullname());
  }

}
