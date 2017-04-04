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
import org.schedoscope.metascope.model.MetascopeCategoryObject;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)

public class MetascopeTableRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeTableRepository metascopeTableRepository;

  private MetascopeCategoryObject metascopeCategoryObject;
  private MetascopeComment metascopeComment;

  @Before
  public void setup() {
    MetascopeCategoryObject tmpMetascopeCategoryObject = new MetascopeCategoryObject();
    tmpMetascopeCategoryObject.setName("some object");
    this.metascopeCategoryObject = this.entityManager.persist(tmpMetascopeCategoryObject);

    MetascopeComment tmpMetascopeComment = new MetascopeComment();
    tmpMetascopeComment.setText("some comment");
    this.metascopeComment = this.entityManager.persist(tmpMetascopeComment);

    MetascopeTable testTable = new MetascopeTable();
    testTable.setFqdn("testTableId");
    testTable.setPersonResponsible("alice");
    testTable.setViewCount(5);

    List<MetascopeCategoryObject> metascopeCategoryObjects = new ArrayList<>();
    metascopeCategoryObjects.add(metascopeCategoryObject);
    testTable.setCategoryObjects(metascopeCategoryObjects);

    Set<MetascopeComment> metascopeComments = new HashSet<>();
    metascopeComments.add(metascopeComment);
    testTable.setComments(metascopeComments);

    this.entityManager.persist(testTable);

    MetascopeTable bobTable = new MetascopeTable();
    bobTable.setFqdn("bobTableId");
    bobTable.setPersonResponsible("bob");
    bobTable.setViewCount(4);
    this.entityManager.persist(bobTable);

    MetascopeTable otherTable = new MetascopeTable();
    otherTable.setFqdn("otherTableId");
    otherTable.setPersonResponsible("alice");
    otherTable.setViewCount(3);
    this.entityManager.persist(otherTable);

    MetascopeTable tmpTable1 = new MetascopeTable();
    tmpTable1.setFqdn("tmpTable1");
    tmpTable1.setViewCount(2);
    this.entityManager.persist(tmpTable1);

    MetascopeTable tmpTable2 = new MetascopeTable();
    tmpTable2.setFqdn("tmpTable2");
    tmpTable2.setViewCount(1);
    this.entityManager.persist(tmpTable2);

    MetascopeTable tmpTable3 = new MetascopeTable();
    tmpTable3.setFqdn("tmpTable3");
    tmpTable3.setViewCount(0);
    this.entityManager.persist(tmpTable3);
  }

  @Test
  public void findByCategoryObjectTest() {
    List<MetascopeTable> tables = metascopeTableRepository.findByCategoryObject(metascopeCategoryObject);

    assertEquals(1, tables.size());
    assertEquals("testTableId", tables.get(0).getFqdn());
  }

  @Test
  public void findByCommentTest() {
    MetascopeTable table = metascopeTableRepository.findByComment(metascopeComment);

    assertNotNull(table);
    assertEquals("testTableId", table.getFqdn());
  }

  @Test
  public void getAllOwnerTest() {
    Set<String> owner = metascopeTableRepository.getAllOwner();

    assertNotNull(owner);
    assertEquals(2, owner.size());

    Iterator<String> iterator = owner.iterator();
    String firstOwner = iterator.next();
    String secondOwner = iterator.next();

    assertTrue((firstOwner.equals("alice") && secondOwner.equals("bob"))
      || (firstOwner.equals("bob") && secondOwner.equals("alice")));
  }

  @Test
  public void findTop5ByOrderByViewCountDescTest() {
    List<MetascopeTable> top5 = metascopeTableRepository.findTop5ByOrderByViewCountDesc();

    assertNotNull(top5);
    assertEquals(5, top5.size());
    int index = 0;
    for (int i = 5; i >= 1; i--) {
      MetascopeTable metascopeTable = top5.get(index++);
      assertEquals(i, metascopeTable.getViewCount());
    }
  }

  @Test
  public void getAllTablesNamesTest() {
    List<String> allTablesNames = metascopeTableRepository.getAllTablesNames();

    assertNotNull(allTablesNames);
    assertEquals(6, allTablesNames.size());
    assertTrue(allTablesNames.contains("testTableId"));
    assertTrue(allTablesNames.contains("bobTableId"));
    assertTrue(allTablesNames.contains("otherTableId"));
    assertTrue(allTablesNames.contains("tmpTable1"));
    assertTrue(allTablesNames.contains("tmpTable2"));
    assertTrue(allTablesNames.contains("tmpTable3"));
  }

}
