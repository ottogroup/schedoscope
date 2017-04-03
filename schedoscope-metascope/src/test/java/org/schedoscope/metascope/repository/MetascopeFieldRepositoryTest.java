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
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        Set<MetascopeComment> comments = new HashSet<>();
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
        assertEquals("comment", metascopeField.getComments().iterator().next().getText());
    }

}
