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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.schedoscope.metascope.SpringTest;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.service.UserEntityService;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentationServiceTest extends SpringTest {

  private static final String DOCUMENATION_1 = "Some plain text";
  private static final String DOCUMENTATION_2 = "Some other plain text";
  private static final String COMMENT = "Some comment on a documenation";

  @Before
  public void setup() {
    /* ### set up mocks ### */

    /*
     * mock UserEntityService getUser() method, which in production returns the
     * logged in user
     */
    UserEntityService userEntityServiceMock = mock(UserEntityService.class);
    when(userEntityServiceMock.getUser()).thenReturn(getLoggedInUser());
    mockField(documentationService, "userEntityService", userEntityServiceMock);

    /* mock the calls to solr (Solr is tested seperatly) */
    SolrFacade solrFacadeMock = mock(SolrFacade.class);
    mockField(tableEntityService, "solr", solrFacadeMock);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void documentationService_01_updateDocumentation_createTableDocumenation() {
    TableEntity tableEntity = getTestTable();

    assertTrue(tableEntity.getComment() == null);

    documentationService.updateDocumentation(tableEntity, DOCUMENATION_1);

    tableEntity = getTestTable();
    assertTrue(tableEntity.getComment() != null);
    assertEquals(tableEntity.getComment().getPlainText(), DOCUMENATION_1);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void documentationService_02_updateDocumentation_updateTableDocumenation() {
    TableEntity tableEntity = getTestTable();

    assertTrue(tableEntity.getComment() != null);
    assertEquals(tableEntity.getComment().getPlainText(), DOCUMENATION_1);

    documentationService.updateDocumentation(tableEntity, DOCUMENTATION_2);

    tableEntity = getTestTable();
    assertTrue(tableEntity.getComment() != null);
    assertEquals(tableEntity.getComment().getPlainText(), DOCUMENTATION_2);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void documentationService_03_addComment() {
    TableEntity tableEntity = getTestTable();

    assertTrue(tableEntity.getComment() != null);
    assertEquals(tableEntity.getComment().getPlainText(), DOCUMENTATION_2);
    assertEquals(tableEntity.getComments().size(), 0);

    documentationService.addComment(tableEntity, COMMENT);

    tableEntity = getTestTable();
    List<CommentEntity> comments = tableEntity.getComments();

    assertEquals(comments.size(), 1);
    assertEquals(comments.get(0).getPlainText(), COMMENT);
  }

  @Test
  @Transactional
  @Rollback(false)
  public void documentationService_04_deleteComment() {
    TableEntity tableEntity = getTestTable();
    List<CommentEntity> comments = tableEntity.getComments();
    CommentEntity commentEntity = comments.get(0);

    assertEquals(comments.size(), 1);
    assertEquals(commentEntity.getPlainText(), COMMENT);

    documentationService.deleteComment(tableEntity, commentEntity);

    comments = getTestTable().getComments();

    assertEquals(comments.size(), 0);
  }

}
