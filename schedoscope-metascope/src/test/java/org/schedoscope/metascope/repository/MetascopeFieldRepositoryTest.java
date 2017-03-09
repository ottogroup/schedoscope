package org.schedoscope.metascope.repository;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@DataJpaTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class MetascopeFieldRepositoryTest {

  @Autowired
  private TestEntityManager entityManager;

  @Autowired
  private MetascopeFieldRepository metascopeFieldRepository;
  private MetascopeComment metascopeComment;

  @Before
  public void setup() {
    /* create test data */
    MetascopeComment tmpComment = new MetascopeComment();
    tmpComment.setText("comment");
    this.metascopeComment = this.entityManager.persist(tmpComment);

    MetascopeField yearParameter = new MetascopeField();
    yearParameter.setFieldId("1");
    yearParameter.setFieldName("year");
    yearParameter.setParameter(true);
    this.entityManager.persist(yearParameter);

    MetascopeField yearParameter2 = new MetascopeField();
    yearParameter2.setFieldId("2");
    yearParameter2.setFieldName("year");
    yearParameter2.setParameter(true);
    this.entityManager.persist(yearParameter2);

    MetascopeField monthParameter = new MetascopeField();
    monthParameter.setFieldId("3");
    monthParameter.setFieldName("month");
    monthParameter.setParameter(true);
    this.entityManager.persist(monthParameter);

    MetascopeField someField = new MetascopeField();
    someField.setFieldId("4");
    someField.setFieldName("someField");
    someField.setParameter(false);
    List<MetascopeComment> comments = new ArrayList<>();
    comments.add(metascopeComment);
    someField.setComments(comments);

    this.entityManager.persist(someField);
  }

  @Test
  public void findDistinctParameterTest() {
    List<Object[]> distinctParameters = metascopeFieldRepository.findDistinctParameters();

    assertNotNull(distinctParameters);
    assertEquals(2, distinctParameters.size());
  }

  @Test
  public void findByComment() {
    MetascopeField metascopeField = metascopeFieldRepository.findByComment(metascopeComment);

    assertNotNull(metascopeField);
    assertEquals("4", metascopeField.getFieldId());
    assertEquals("someField", metascopeField.getFieldName());
    assertEquals("comment", metascopeField.getComments().get(0).getText());
  }

}
