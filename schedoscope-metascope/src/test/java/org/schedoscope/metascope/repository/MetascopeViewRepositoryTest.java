package org.schedoscope.metascope.repository;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeTaxonomy;
import org.schedoscope.metascope.model.MetascopeView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeViewRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeViewRepository metascopeViewRepository;

  private MetascopeTable metascopeTable;
  private MetascopeView metascopeViewFirst;
  private MetascopeView metascopeViewSecond;
  private Pageable pageable;

  @Before
  public void setup() {
    MetascopeView tmpMetascopeViewFirst = new MetascopeView();
    tmpMetascopeViewFirst.setViewId("1");
    tmpMetascopeViewFirst.setParameterString("/year/month");
    this.metascopeViewFirst = this.entityManager.persist(tmpMetascopeViewFirst);

    MetascopeView tmpMetascopeViewSecond = new MetascopeView();
    tmpMetascopeViewSecond.setViewId("2");
    tmpMetascopeViewSecond.setParameterString("/year/month/day");
    this.metascopeViewSecond = this.entityManager.persist(tmpMetascopeViewSecond);

    MetascopeTable tmpMetascopeTable = new MetascopeTable();
    tmpMetascopeTable.setFqdn("tableFqdn");
    this.metascopeTable = this.entityManager.persist(tmpMetascopeTable);

    metascopeViewFirst.setTable(metascopeTable);
    this.metascopeViewFirst = this.entityManager.persist(metascopeViewFirst);

    metascopeViewSecond.setTable(metascopeTable);
    this.metascopeViewSecond = this.entityManager.persist(metascopeViewSecond);

    List<MetascopeView> views = new ArrayList<>();
    views.add(metascopeViewFirst);
    views.add(metascopeViewSecond);
    metascopeTable.setViews(views);
    this.metascopeTable = this.entityManager.persist(metascopeTable);

    this.pageable = new PageRequest(1, 10);
  }

  @Test
  public void getPartitionPositionTest() {
    int partitionPositionFirst = metascopeViewRepository.getPartitionPosition(metascopeTable.getFqdn(), "1");
    int partitionPositionSecond = metascopeViewRepository.getPartitionPosition(metascopeTable.getFqdn(), "2");

    assertEquals(partitionPositionFirst, 0);
    assertEquals(partitionPositionSecond, 1);
  }

  @Test
  public void findParameterStringsForTableTest() {
    List<String> parameterStrings = metascopeViewRepository.findParameterStringsForTable(metascopeTable.getFqdn());

    assertNotNull(parameterStrings);
    assertEquals(2, parameterStrings.size());
    assertTrue(parameterStrings.get(0).equals("/year/month") && parameterStrings.get(1).equals("/year/month/day")
            || parameterStrings.get(1).equals("/year/month") && parameterStrings.get(0).equals("/year/month/day") );
  }

  @Test
  public void findByTableFqdnOrderByViewIdTest() {
    Page<MetascopeView> views = metascopeViewRepository.findByTableFqdnOrderByViewId(metascopeTable.getFqdn(), pageable);

    assertNotNull(views);
    assertEquals(2, views.getTotalElements());
  }

  @Test
  public void findFirstByTableFqdnTest() {
    MetascopeView view = metascopeViewRepository.findFirstByTableFqdn(metascopeTable.getFqdn());

    assertNotNull(view);
    assertTrue(view.getViewId().equals("1") || view.getViewId().equals("2"));
  }

}
