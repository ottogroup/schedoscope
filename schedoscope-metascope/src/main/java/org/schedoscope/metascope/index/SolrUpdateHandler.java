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
package org.schedoscope.metascope.index;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeView;
import org.springframework.scheduling.annotation.AsyncResult;

import java.io.IOException;
import java.util.concurrent.Future;

public class SolrUpdateHandler {

  public static final String ID = "id";
  public static final String TYPE = "type";
  public static final String SCHEDOSCOPE_ID = "schedoscopeId";
  public static final String DATABASE_NAME = "databaseName";
  public static final String TABLE_NAME = "tableName";
  public static final String FIELDS = "fields";
  public static final String PARAMETERS = "parameters";
  public static final String TAXONOMIES = "taxonomies";
  public static final String CATEGORIES = "categories";
  public static final String CATEGORIE_OBJECTSS = "categoryObjects";
  public static final String STORAGE_FORMAT = "storageFormat";
  public static final String TRANSFORMATION = "transformation";
  public static final String PERSON_RESPONSIBLE = "personResponsible";
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
   * @param table  table entity to update
   * @param commit immediately commit change to index
   */
  public void updateTableEntity(MetascopeTable table, boolean commit) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(ID, table.getFqdn());
    doc.setField(TYPE, TYPE_TABLE);
    doc.setField(SCHEDOSCOPE_ID, table.getSchedoscopeId());
    doc.setField(DATABASE_NAME, table.getDatabaseName());
    doc.setField(TABLE_NAME, table.getTableName());
    doc.setField(FIELDS, table.getFieldNames());
    doc.setField(PARAMETERS, table.getParameterNames());
    doc.setField(TRANSFORMATION, table.getTransformation().getTransformationType().split(" -> ")[0]);
    doc.setField(PERSON_RESPONSIBLE, table.getPersonResponsible());
    doc.setField(EXPORTS, table.getExportNames());
    doc.setField(STORAGE_FORMAT, table.getStorageFormat());
    doc.setField(MATERIALIZE_ONCE, table.isMaterializeOnce());
    doc.setField(EXTERNAL, table.isExternalTable());
    doc.setField(CREATED_AT, table.getCreatedAt());
    doc.setField(OWNER, table.getTableOwner());
    doc.setField(DESCRIPTION, table.getTableDescription());
    doc.setField(TAXONOMIES, table.getTaxonomyNames());
    doc.setField(CATEGORIES, table.getCategoryNames());
    doc.setField(CATEGORIE_OBJECTSS, table.getCategoryObjectNames());
    doc.setField(TAGS, table.getTags());
    if (table.getComment() != null) {
      doc.setField(DOCUMENTATION, table.getComment().getPlainText());
      String comments = "";
      for (MetascopeComment comment : table.getComments()) {
        if (!comments.isEmpty()) {
          comments += "     ";
        }
        comments += comment.getUsername() + ": " + comment.getPlainText();
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
   * @param table  table entity to update
   * @param commit immediately commit change to index
   */
  public Future<Void> updateTableEntityAsync(MetascopeTable table, boolean commit) {
    updateTableEntity(table, commit);
    return new AsyncResult<Void>(null);
  }

  /**
   * Updates the Solr document for the given table entity. In contrast to
   * {@link SolrUpdateHandler#updateTableEntity(MetascopeTable, boolean)}, only
   * some specific fields are updated
   *
   * @param table  table entity to update
   * @param commit immediately commit change to index
   */
  public void updateTablePartial(MetascopeTable table, boolean commit) {
    SolrInputDocument doc = getDocument(table.getFqdn());
    if (doc == null) {
      doc = new SolrInputDocument();
    }
    doc.setField(ID, table.getFqdn());
    doc.setField(TYPE, TYPE_TABLE);
    doc.setField(SCHEDOSCOPE_ID, table.getSchedoscopeId());
    doc.setField(DATABASE_NAME, table.getDatabaseName());
    doc.setField(TABLE_NAME, table.getTableName());
    if (table.getFields().size() > 0) {
      doc.setField(FIELDS, table.getFieldNames());
    }
    doc.setField(TRANSFORMATION, table.getTransformation().getTransformationType().split(" -> ")[0]);
    if (table.getExports() != null) {
      doc.setField(EXPORTS, table.getExportNames());
    }
    doc.setField(STORAGE_FORMAT, table.getStorageFormat());
    doc.setField(MATERIALIZE_ONCE, table.isMaterializeOnce());
    doc.setField(EXTERNAL, table.isExternalTable());
    doc.setField(DESCRIPTION, table.getTableDescription());
    if (table.getTableOwner() != null) {
      doc.setField(OWNER, table.getTableOwner());
    }
    if (table.getCreatedAt() != 0) {
      doc.setField(CREATED_AT, table.getCreatedAt() / 1000);
    }
    if (table.getLastTransformation() != 0) {
      doc.setField(TRANSFORMATIONTIMESTAMP, table.getLastTransformation() / 1000);
    }
    if (table.getTaxonomyNames() != null || table.getTaxonomyNames().size() > 0) {
      doc.setField(TAXONOMIES, table.getTaxonomyNames());
    }
    if (table.getCategoryNames() != null || table.getCategoryNames().size() > 0) {
      doc.setField(CATEGORIES, table.getCategoryNames());
    }
    if (table.getCategoryObjectNames() != null || table.getCategoryObjectNames().size() > 0) {
      doc.setField(CATEGORIE_OBJECTSS, table.getCategoryObjectNames());
    }
    addDocument(doc);
    if (commit) {
      commit();
    }
  }

  /**
   * Updates the Solr document for the given view entity
   *
   * @param view   view entity to update
   * @param commit immediately commit change to index
   */
  public void updateViewEntity(MetascopeView view, boolean commit) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(ID, view.getViewId());
    doc.setField(TYPE, TYPE_PARTITION);
    doc.addField(TRANSFORMATIONTIMESTAMP, view.getLastTransformation() / 1000);
    doc.setField(SCHEDOSCOPE_ID, view.getTable().getSchedoscopeId());
    doc.addField(TABLE_NAME, view.getTable().getTableName());
    doc.addField(DATABASE_NAME, view.getTable().getDatabaseName());
    doc.addField(CREATED_AT, view.getTable().getCreatedAt());

    if (view.getParameterString() != null && !view.getParameterString().isEmpty()) {
      doc.addField(PARAMETERSTRING, view.getParameterString());
      String[] params = view.getParameterString().replace("/", " ").trim().split(" ");
      for (String param : params) {
        String[] keyAndValue = param.split("=");
        doc.addField(keyAndValue[0] + "_s", keyAndValue[1]);
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
   * @param view   view entity to update
   * @param commit immediately commit change to index
   */
  public Future<Void> updateViewEntityAsync(MetascopeView view, boolean commit) {
    updateViewEntity(view, commit);
    return new AsyncResult<Void>(null);
  }

  /**
   * Updates the Solr document for the given view entity. In contrast to
   * {@link SolrUpdateHandler#updateViewEntity(MetascopeView, boolean)}, only the
   * status, transformationEnd and createdAt fields are updated
   *
   * @param view   view entity to update
   * @param commit immediately commit change to index
   */
  public void updateViewStatusInformation(MetascopeView view, Long transformationEnd, Long createdAt, boolean commit) {
    SolrInputDocument doc = getDocument(view.getViewId());
    if (doc == null) {
      doc = new SolrInputDocument();
    }
    doc.setField(ID, view.getViewId());
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
   * @param id id of the document
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
   * @param doc solr document to be added
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
