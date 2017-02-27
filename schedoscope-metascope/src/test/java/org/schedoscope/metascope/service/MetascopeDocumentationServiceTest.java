package org.schedoscope.metascope.service;
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.MetascopeActivity;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeUser;
import org.schedoscope.metascope.repository.MetascopeCommentRepository;
import org.schedoscope.metascope.repository.MetascopeFieldRepository;
import org.schedoscope.metascope.repository.MetascopeTableRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class MetascopeDocumentationServiceTest {

    /* service to test */
    private MetascopeDocumentationService metascopeDocumentationService;

    /* mocked fields */
    private MetascopeUserService metascopeUserService;
    private MetascopeActivityService metascopeActivityService;
    private MetascopeCommentRepository metascopeCommentRepository;
    private MetascopeTableRepository metascopeTableRepository;
    private MetascopeFieldRepository metascopeFieldRepository;
    private SolrFacade solrFacade;

    /* database stubs */
    private Set<MetascopeComment> commentDatabaseStub;
    private Set<MetascopeActivity> activityDatabaseStub;

    /* test data */
    private MetascopeTable metascopeTable;
    private MetascopeUser metascopeUser;
    private MetascopeComment metascopeComment;
    private MetascopeActivity metascopeActivity;

    @Before
    public void setup() {
        this.metascopeDocumentationService = new MetascopeDocumentationService();
        this.metascopeUserService = mock(MetascopeUserService.class);
        this.metascopeActivityService = mock(MetascopeActivityService.class);
        this.metascopeCommentRepository = mock(MetascopeCommentRepository.class);
        this.metascopeTableRepository = mock(MetascopeTableRepository.class);
        this.metascopeFieldRepository = mock(MetascopeFieldRepository.class);
        this.solrFacade = mock(SolrFacade.class);
        this.commentDatabaseStub = new HashSet<>();
        this.activityDatabaseStub = new HashSet<>();

        metascopeDocumentationService.setMetascopeUserService(metascopeUserService);
        metascopeDocumentationService.setMetascopeActivityService(metascopeActivityService);
        metascopeDocumentationService.setMetascopeCommentRepository(metascopeCommentRepository);
        metascopeDocumentationService.setMetascopeFieldRepository(metascopeFieldRepository);
        metascopeDocumentationService.setMetascopeTableRepository(metascopeTableRepository);
        metascopeDocumentationService.setSolr(solrFacade);

    /* test data */
        this.metascopeTable = new MetascopeTable();
        metascopeTable.setComments(new ArrayList<MetascopeComment>());

        this.metascopeUser = new MetascopeUser();
        metascopeUser.setUsername("testUser");

        this.metascopeComment = new MetascopeComment();
        metascopeComment.setId(1l);
        metascopeComment.setUsername(metascopeUser.getUsername());
        metascopeComment.setText("testComment");

        this.metascopeActivity = new MetascopeActivity();
        metascopeActivity.setTable(metascopeTable);
        metascopeActivity.setType(MetascopeActivity.ActivityType.NEW_COMMENT);
        metascopeActivity.setUsername(metascopeUser.getUsername());
        metascopeActivity.setActivityId("1");

    /* method stubs */
        doAnswerCreateActivity(metascopeActivity);
        doAnswerCreateComment(metascopeComment);
        doAnswerDeleteComment(metascopeComment);
    }

    @Test
    public void findByIdTest() {
    /* test data */
        MetascopeComment metascopeComment = new MetascopeComment();
        metascopeComment.setId(2l);
        commentDatabaseStub.add(metascopeComment);

        when(metascopeCommentRepository.findOne(any(Long.class))).thenReturn(commentDatabaseStub.iterator().next());

        MetascopeComment metascopeCommentById = metascopeDocumentationService.findById("1");

        assertNotNull(metascopeCommentById);
        assertEquals(metascopeComment, metascopeCommentById);
    }

    @Test
    public void updateDocumentationTest() {
    /* pre assertions */
        assertEquals(0, commentDatabaseStub.size());
        assertEquals(0, activityDatabaseStub.size());

    /* method to test */
        metascopeDocumentationService.addComment(metascopeTable, metascopeComment.getText(), metascopeUser);

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals("1", activityDatabaseStub.iterator().next().getActivityId());
        assertEquals(1, commentDatabaseStub.size());
        assertEquals("testComment", metascopeComment.getText());
    }

    @Test
    public void addCommentTest() {
    /* pre assertions */
        assertEquals(0, commentDatabaseStub.size());
        assertEquals(0, activityDatabaseStub.size());

    /* method to test */
        metascopeDocumentationService.addComment(metascopeTable, metascopeComment.getText(), metascopeUser);

    /* post assertions */
        assertEquals(1, activityDatabaseStub.size());
        assertEquals("1", activityDatabaseStub.iterator().next().getActivityId());
        assertEquals(1, commentDatabaseStub.size());
        assertEquals("testComment", metascopeComment.getText());
    }

    @Test
    public void editCommentTest() {
    /* comment already is saved in database */
        commentDatabaseStub.add(metascopeComment);

    /* pre assertions */
        assertEquals(1, commentDatabaseStub.size());
        assertEquals("testComment", metascopeComment.getText());

    /* method to test */
        metascopeDocumentationService.editComment(metascopeComment, "some new comment", metascopeUser);

    /* post assertions */
        assertEquals(1, commentDatabaseStub.size());
        assertEquals("some new comment", metascopeComment.getText());
    }

    @Test
    public void deleteCommentTest() {
    /* comment already is saved in database */
        commentDatabaseStub.add(metascopeComment);

    /* pre assertions */
        assertEquals(1, commentDatabaseStub.size());
        assertEquals("testComment", metascopeComment.getText());

    /* method to test */
        metascopeDocumentationService.deleteComment(metascopeTable, metascopeComment, metascopeUser);

    /* post assertions */
        assertEquals(0, commentDatabaseStub.size());
    }

    private void doAnswerCreateActivity(final MetascopeActivity metascopeActivity) {
        Answer answer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                activityDatabaseStub.add(metascopeActivity);
                return metascopeActivity;
            }
        };
        doAnswer(answer).when(metascopeActivityService).createUpdateDocumentActivity(any(MetascopeTable.class), any(String.class));
        doAnswer(answer).when(metascopeActivityService).createUpdateTableMetadataActivity(any(MetascopeTable.class), any(String.class));
        doAnswer(answer).when(metascopeActivityService).createNewCommentActivity(any(MetascopeTable.class), any(String.class));
        doAnswer(answer).when(metascopeActivityService).createUpdateTaxonomyActivity(any(MetascopeTable.class), any(String.class));
    }

    private void doAnswerCreateComment(final MetascopeComment metascopeComment) {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                commentDatabaseStub.add(metascopeComment);
                return metascopeComment;
            }
        }).when(metascopeCommentRepository).save(any(MetascopeComment.class));
    }

    private void doAnswerDeleteComment(final MetascopeComment metascopeComment) {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                commentDatabaseStub.remove(metascopeComment);
                return metascopeComment;
            }
        }).when(metascopeCommentRepository).delete(any(MetascopeComment.class));
    }

}
