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
package org.schedoscope.metascope.index;

import java.io.IOException;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.springframework.scheduling.annotation.AsyncResult;

public class SolrUpdateHandler {

  public static final String ID = "id";
  public static final String TYPE = "type";
  public static final String DATABASE_NAME = "databaseName";
  public static final String TABLE_NAME = "tableName";
  public static final String FIELDS = "fields";
  public static final String CATEGORIES = "categories";
  public static final String BUSINESS_OBJECTS = "bos";
  public static final String STORAGE_FORMAT = "storageFormat";
  public static final String TRANSFORMATION = "transformation";
  public static final String EXPORTS = "exports";
  public static final String TRANSFORMATIONTIMESTAMP = "transformationTimestamp";
  public static final String MATERIALIZE_ONCE = "materializeOnce";
  public static final String EXTERNAL = "external";
  public static final String CREATED_AT = "createdAt";
  public static final String OWNER = "owner";
  public static final String DESCRIPTION = "description";
  public static final String TAGS = "tags";
  public static final String DOCUMENTATION = "documentation";
  public static final String COMMENTS = "comments";
  public static final String STATUS = "status";
  public static final String PARAMETERSTRING = "parameterString";
  public static final String TYPE_TABLE = "Table";
  public static final String TYPE_PARTITION = "Partition";

  private SolrClient solrClient;

  public SolrUpdateHandler(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  /**
   * Updates the Solr document for the given table entity
   * 
   * @param tableEntity
   *          table entity to update
   * @param commit
   *          immediately commit change to index
   */
  public void updateTableEntity(TableEntity tableEntity, boolean commit) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(ID, tableEntity.getFqdn());
    doc.setField(TYPE, TYPE_TABLE);
    doc.setField(DATABASE_NAME, tableEntity.getDatabaseName());
    doc.setField(TABLE_NAME, tableEntity.getTableName());
    doc.setField(FIELDS, tableEntity.getFieldNames());
    doc.setField(TRANSFORMATION, tableEntity.getTransformationType().split(" -> ")[0]);
    doc.setField(EXPORTS, tableEntity.getExportNames());
    doc.setField(STORAGE_FORMAT, tableEntity.getStorageFormat());
    doc.setField(MATERIALIZE_ONCE, tableEntity.isMaterializeOnce());
    doc.setField(EXTERNAL, tableEntity.isExternalTable());
    doc.setField(CREATED_AT, tableEntity.getCreatedAt());
    doc.setField(OWNER, tableEntity.getTableOwner());
    doc.setField(DESCRIPTION, tableEntity.getTableDescription());
    doc.setField(CATEGORIES, tableEntity.getCategoryNames());
    doc.setField(BUSINESS_OBJECTS, tableEntity.getBusinessObjectNames());
    doc.setField(TAGS, tableEntity.getTags());
    doc.setField(STATUS, tableEntity.getStatus());
    if (tableEntity.getComment() != null) {
      doc.setField(DOCUMENTATION, tableEntity.getComment().getPlainText());
      String comments = "";
      for (CommentEntity comment : tableEntity.getComments()) {
        if (!comments.isEmpty()) {
          comments += "     ";
        }
        comments += comment.getUser().getFullname() + ": " + comment.getPlainText();
      }
      if (!comments.isEmpty()) {
        doc.setField(COMMENTS, comments);
      }
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Updates the Solr document for the given table entity asynchronly
   * 
   * @param tableEntity
   *          table entity to update
   * @param commit
   *          immediately commit change to index
   */
  public Future<Void> updateTableEntityAsync(TableEntity tableEntity, boolean commit) {
    updateTableEntity(tableEntity, commit);
    return new AsyncResult<Void>(null);
  }

  /**
   * Updates the Solr document for the given table entity. In contrast to
   * {@link SolrUpdateHandler#updateTableEntity(TableEntity, boolean)}, only
   * some specific fields are updated
   * 
   * @param tableEntity
   *          table entity to update
   * @param commit
   *          immediately commit change to index
   */
  public void updateTablePartial(TableEntity tableEntity, boolean commit) {
    SolrInputDocument doc = getDocument(tableEntity.getFqdn());
    if (doc == null) {
      doc = new SolrInputDocument();
    }
    doc.setField(ID, tableEntity.getFqdn());
    doc.setField(TYPE, TYPE_TABLE);
    doc.setField(DATABASE_NAME, tableEntity.getDatabaseName());
    doc.setField(TABLE_NAME, tableEntity.getTableName());
    if (tableEntity.getFields().size() > 0) {
      doc.setField(FIELDS, tableEntity.getFieldNames());
    }
    doc.setField(TRANSFORMATION, tableEntity.getTransformationType().split(" -> ")[0]);
    if (tableEntity.getExports() != null) {
      doc.setField(EXPORTS, tableEntity.getExportNames());
    }
    doc.setField(STORAGE_FORMAT, tableEntity.getStorageFormat());
    doc.setField(MATERIALIZE_ONCE, tableEntity.isMaterializeOnce());
    doc.setField(EXTERNAL, tableEntity.isExternalTable());
    doc.setField(DESCRIPTION, tableEntity.getTableDescription());
    doc.setField(STATUS, tableEntity.getStatus());
    if (tableEntity.getTableOwner() != null) {
      doc.setField(OWNER, tableEntity.getTableOwner());
    }
    if (tableEntity.getCreatedAt() != 0) {
      doc.setField(CREATED_AT, tableEntity.getCreatedAt() / 1000);
    }
    if (tableEntity.getLastTransformation() != 0) {
      doc.setField(TRANSFORMATIONTIMESTAMP, tableEntity.getLastTransformation() / 1000);
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Updates the Solr document for the given table entity. In contrast to
   * {@link SolrUpdateHandler#updateTableEntity(TableEntity, boolean)}, only the
   * status and lastTransformation fields are updated
   * 
   * @param tableEntity
   *          table entity to update
   * @param commit
   *          immediately commit change to index
   */
  public void updateTableStatusInformation(TableEntity tableEntity, Long lastTransformation, boolean commit) {
    SolrInputDocument doc = getDocument(tableEntity.getFqdn());
    if (doc == null) {
      doc = new SolrInputDocument();
    }
    doc.setField(ID, tableEntity.getFqdn());
    doc.setField(STATUS, tableEntity.getStatus());
    if (lastTransformation != null) {
      doc.setField(TRANSFORMATIONTIMESTAMP, lastTransformation / 1000);
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Updates the Solr document for the given view entity
   * 
   * @param viewEntity
   *          view entity to update
   * @param commit
   *          immediately commit change to index
   */
  public void updateViewEntity(ViewEntity viewEntity, boolean commit) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(ID, viewEntity.getUrlPath());
    doc.setField(TYPE, TYPE_PARTITION);
    doc.addField(STATUS, viewEntity.getStatus());
    doc.addField(TRANSFORMATIONTIMESTAMP, viewEntity.getTransformationEnd() / 1000);
    doc.addField(TABLE_NAME, viewEntity.getTable().getTableName());
    doc.addField(DATABASE_NAME, viewEntity.getTable().getDatabaseName());
    doc.addField(CREATED_AT, viewEntity.getTable().getCreatedAt());
    if (viewEntity.getParameters() != null) {
      if (viewEntity.getParameters().size() > 0) {
        doc.addField(PARAMETERSTRING, viewEntity.getParameterString());
      }
      for (ParameterValueEntity param : viewEntity.getParameters()) {
        doc.addField(param.getKey() + "_s", param.getValue());
      }
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Updates the Solr document for the given view entity asynchronly
   * 
   * @param viewEntity
   *          view entity to update
   * @param commit
   *          immediately commit change to index
   */
  public Future<Void> updateViewEntityAsync(ViewEntity viewEntity, boolean commit) {
    updateViewEntity(viewEntity, commit);
    return new AsyncResult<Void>(null);
  }

  /**
   * Updates the Solr document for the given view entity. In contrast to
   * {@link SolrUpdateHandler#updateViewEntity(ViewEntity, boolean)}, only the
   * status, transformationEnd and createdAt fields are updated
   * 
   * @param viewEntity
   *          view entity to update
   * @param commit
   *          immediately commit change to index
   */
  public void updateViewStatusInformation(ViewEntity viewEntity, Long transformationEnd, Long createdAt, boolean commit) {
    SolrInputDocument doc = getDocument(viewEntity.getUrlPath());
    if (doc == null) {
      doc = new SolrInputDocument();
    }
    doc.setField(ID, viewEntity.getUrlPath());
    doc.setField(STATUS, viewEntity.getStatus());
    if (transformationEnd != null) {
      doc.setField(TRANSFORMATIONTIMESTAMP, transformationEnd / 1000);
    }
    if (createdAt != null) {
      doc.setField(CREATED_AT, createdAt / 1000);
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Retrieves a document from solr index
   * 
   * @param id
   *          id of the document
   * @return solr document with given id
   */
  public SolrInputDocument getDocument(String id) {
    SolrInputDocument doc = null;
    try {
      SolrDocument storedDoc = solrClient.getById(id);
      if (storedDoc != null) {
        doc = ClientUtils.toSolrInputDocument(storedDoc);
      }
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return doc;
  }

  /**
   * Adds a document to solr index
   * 
   * @param doc
   *          solr document to be added
   */
  private void addDocument(SolrInputDocument doc) {
    try {
      solrClient.add(doc);
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Delete all data stored in solr index
   */
  public void clearSolrData() {
    try {
      solrClient.deleteByQuery("*:*");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Commit all previous changes since last commit call to solr index
   */
  public void commit() {
    try {
      solrClient.commit();
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
  }

}
