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
package org.schedoscope.metascope.model.key;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.ManyToOne;

import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.model.ActivityEntity.ActivityType;

public class ActivityEntityKey implements Serializable {

  private static final long serialVersionUID = 7442974394555530660L;

  @Column
  private ActivityType type;
  @ManyToOne
  private UserEntity user;
  @ManyToOne
  private TableEntity table;

  public ActivityEntityKey() {
  }

  public ActivityEntityKey(ActivityType type, UserEntity user, TableEntity table) {
    this.type = type;
    this.user = user;
    this.table = table;
  }

  public ActivityType getType() {
    return type;
  }

  public void setType(ActivityType type) {
    this.type = type;
  }

  public UserEntity getUser() {
    return user;
  }

  public void setUser(UserEntity user) {
    this.user = user;
  }

  public TableEntity getTable() {
    return table;
  }

  public void setTable(TableEntity table) {
    this.table = table;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ActivityEntityKey other = (ActivityEntityKey) obj;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    if (type != other.type)
      return false;
    if (user == null) {
      if (other.user != null)
        return false;
    } else if (!user.equals(other.user))
      return false;
    return true;
  }

}
