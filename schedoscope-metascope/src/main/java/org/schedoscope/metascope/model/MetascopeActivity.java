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
package org.schedoscope.metascope.model;

import javax.persistence.*;

@Entity
public class MetascopeActivity {

    public enum ActivityType {
        NEW_COMMENT, UPDATE_DOCUMENTATION, UPDATE_TAGS, UPDATE_TABLE_METADATA
    }

    @Id
    private String activityId;
    @Column
    private ActivityType type;
    private String username;
    private long timestamp;
    @ManyToOne
    private MetascopeTable table;

    public String getActivityId() {
        return activityId;
    }

    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    public ActivityType getType() {
        return type;
    }

    public void setType(ActivityType type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public MetascopeTable getTable() {
        return table;
    }

    public void setTable(MetascopeTable table) {
        this.table = table;
    }

}
