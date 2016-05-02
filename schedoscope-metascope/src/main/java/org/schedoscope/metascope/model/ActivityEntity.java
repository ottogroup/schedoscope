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
package org.schedoscope.metascope.model;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

import org.schedoscope.metascope.model.key.ActivityEntityKey;

@Entity
public class ActivityEntity {

  public enum ActivityType {
    NEW_COMMENT, UPDATE_DOCUMENTATION, UPDATE_TAGS, UPDATE_TABLE_METADATA
  }

  @EmbeddedId
  private ActivityEntityKey key;
  private long timestamp;

  public ActivityEntity() {
    this.key = new ActivityEntityKey();
  }

  public ActivityType getType() {
    return key.getType();
  }

  public void setType(ActivityType type) {
    this.key.setType(type);
  }

  public UserEntity getUser() {
    return key.getUser();
  }

  public void setUser(UserEntity user) {
    this.key.setUser(user);
  }

  public TableEntity getTable() {
    return key.getTable();
  }

  public void setTable(TableEntity table) {
    this.key.setTable(table);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

}
