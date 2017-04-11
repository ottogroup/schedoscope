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

import org.jsoup.Jsoup;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.*;
import org.schedoscope.metascope.repository.MetascopeAutoSaveRepository;
import org.schedoscope.metascope.repository.MetascopeCommentRepository;
import org.schedoscope.metascope.repository.MetascopeFieldRepository;
import org.schedoscope.metascope.repository.MetascopeTableRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class MetascopeDocumentationService {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeDocumentationService.class);

  @Autowired
  private MetascopeUserService metascopeUserService;
  @Autowired
  private MetascopeActivityService metascopeActivityService;
  @Autowired
  private MetascopeCommentRepository metascopeCommentRepository;
  @Autowired
  private MetascopeTableRepository metascopeTableRepository;
  @Autowired
  private MetascopeFieldRepository metascopeFieldRepository;
  @Autowired
  private MetascopeAutoSaveRepository metascopeAutoSaveRepository;
  @Autowired
  private SolrFacade solr;

  public MetascopeComment findById(String commentID) {
    return metascopeCommentRepository.findOne(Long.parseLong(commentID));
  }

  @Transactional
  public void updateDocumentation(Documentable documentable, String documentText, MetascopeUser user) {
    if (documentable == null) {
      return;
    }

    if (documentText != null && !documentText.isEmpty()) {
      MetascopeComment comment = documentable.getComment();
      if (comment == null) {
        comment = new MetascopeComment();
        documentable.setComment(comment);
      }

      comment.setText(documentText);
      comment.setPlainText(Jsoup.parse(documentText).body().text());
      comment.setUsername(user.getUsername());
      comment.setLastEdit(System.currentTimeMillis());
      metascopeCommentRepository.save(comment);
      documentable.setComment(comment);
    }

    saveEntity(documentable, user);

    if (documentable instanceof MetascopeTable) {
      metascopeActivityService.createUpdateDocumentActivity(((MetascopeTable) documentable), user.getUsername());
      metascopeAutoSaveRepository.deleteByTableFqdn(((MetascopeTable) documentable).getFqdn());
    } else if (documentable instanceof MetascopeField) {
      MetascopeTable tableEntity = ((MetascopeField) documentable).getTable();
      metascopeActivityService.createUpdateDocumentActivity(tableEntity, user.getUsername());
    }
  }

  @Transactional
  public void addComment(Documentable documentable, String commentText, MetascopeUser user) {
    if (documentable == null) {
      return;
    }

    if (commentText != null && !commentText.isEmpty()) {
      MetascopeComment comment = new MetascopeComment();
      comment.setText(commentText);
      comment.setPlainText(Jsoup.parse(commentText).body().text());
      comment.setUsername(user.getUsername());
      comment.setLastEdit(System.currentTimeMillis());
      metascopeCommentRepository.save(comment);
      documentable.getComments().add(comment);
    }
    saveEntity(documentable, user);

    if (documentable instanceof MetascopeTable) {
      metascopeActivityService.createNewCommentActivity((MetascopeTable) documentable, user.getUsername());
    } else if (documentable instanceof MetascopeField) {
      MetascopeTable tableEntity = ((MetascopeField) documentable).getTable();
      metascopeActivityService.createNewCommentActivity(tableEntity, user.getUsername());
    }
  }

  @Transactional
  public void editComment(MetascopeComment commentEntity, String commentText, MetascopeUser userEntity) {
    if (commentEntity == null) {
      return;
    }

    if (commentText != null && !commentText.isEmpty()) {
      commentEntity.setText(commentText);
      commentEntity.setPlainText(Jsoup.parse(commentText).body().text());
      commentEntity.setUsername(userEntity.getUsername());
      commentEntity.setLastEdit(System.currentTimeMillis());
      metascopeCommentRepository.save(commentEntity);
    }
  }

  @Transactional
  public void deleteComment(Documentable documentable, MetascopeComment commentEntity, MetascopeUser userEntity) {
    if (documentable == null) {
      return;
    }

    if (commentEntity != null) {
      documentable.getComments().remove(commentEntity);
      saveEntity(documentable, userEntity);
      metascopeCommentRepository.delete(commentEntity);
    }
  }

  @Transactional
  public boolean autosaveDocumentation(MetascopeTable table, String documentation, MetascopeUser user) {
    if (table == null) {
      return false;
    }

    if (documentation == null || documentation.isEmpty()) {
      return false;
    }

    if (table.getComment() != null && table.getComment().getText() != null &&
      documentation.equals(table.getComment().getText())) {
      return false;
    }

    String autosaveId = table.getFqdn() + "." + user.getUsername();
    MetascopeAutoSave metascopeAutoSave = metascopeAutoSaveRepository.findOne(autosaveId);
    if (metascopeAutoSave != null && metascopeAutoSave.getText().equals(documentation)) {
      return false;
    }

    if (metascopeAutoSave == null) {
      metascopeAutoSave = new MetascopeAutoSave();
    }

    metascopeAutoSave.setId(autosaveId);
    metascopeAutoSave.setTableFqdn(table.getFqdn());
    metascopeAutoSave.setText(documentation);
    metascopeAutoSave.setTimestamp(System.currentTimeMillis());
    metascopeAutoSaveRepository.save(metascopeAutoSave);

    return true;
  }

  @Transactional
  public boolean checkForDraft(MetascopeTable table, MetascopeUser user) {
    if (table == null) {
      return false;
    }

    String autosaveId = table.getFqdn() + "." + user.getUsername();
    MetascopeAutoSave metascopeAutoSave = metascopeAutoSaveRepository.findOne(autosaveId);

    if (metascopeAutoSave == null) {
      return false;
    }

    if (table.getComment() != null && table.getComment().getText() != null) {
      if (table.getComment().getLastEdit() > metascopeAutoSave.getTimestamp()) {
        return false;
      }
    }

    return true;
  }

  @Transactional
  public MetascopeAutoSave getDraft(MetascopeTable table, MetascopeUser user) {
    if (table == null) {
      return null;
    }

    String autosaveId = table.getFqdn() + "." + user.getUsername();
    return metascopeAutoSaveRepository.findOne(autosaveId);
  }

  @Transactional
  private void saveEntity(Documentable documentable, MetascopeUser user) {
    if (documentable instanceof MetascopeTable) {
      MetascopeTable tableEntity = (MetascopeTable) documentable;
      metascopeTableRepository.save(tableEntity);
      LOG.info("User '{}' modified comment for table '{}'", user.getUsername(), tableEntity.getFqdn());
      solr.updateTableEntityAsync(tableEntity, true);
    } else if (documentable instanceof MetascopeField) {
      MetascopeField fieldEntity = (MetascopeField) documentable;
      metascopeFieldRepository.save(fieldEntity);
      LOG.info("User '{}' modified comment for field '{}' ({})", user.getUsername(),
        fieldEntity.getFieldName(), fieldEntity.getFieldId());
    }
  }

  public void setMetascopeActivityService(MetascopeActivityService metascopeActivityService) {
    this.metascopeActivityService = metascopeActivityService;
  }

  public void setMetascopeCommentRepository(MetascopeCommentRepository metascopeCommentRepository) {
    this.metascopeCommentRepository = metascopeCommentRepository;
  }

  public void setMetascopeFieldRepository(MetascopeFieldRepository metascopeFieldRepository) {
    this.metascopeFieldRepository = metascopeFieldRepository;
  }

  public void setMetascopeTableRepository(MetascopeTableRepository metascopeTableRepository) {
    this.metascopeTableRepository = metascopeTableRepository;
  }

  public void setMetascopeUserService(MetascopeUserService metascopeUserService) {
    this.metascopeUserService = metascopeUserService;
  }

  public void setSolr(SolrFacade solr) {
    this.solr = solr;
  }

}
