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
package org.schedoscope.metascope.util;

import java.util.ArrayList;
import java.util.List;

public class LineageNode {

  private String label;
  private int id;
  private int level;
  private List<LineageNode> wiredTo;
  private String type;
  private String fqdn;

  public LineageNode() {
    this.wiredTo = new ArrayList<LineageNode>();
  }

  public LineageNode(String label, int level, String type, String fqdn) {
    this();
    this.label = label;
    this.level = level;
    this.type = type;
    this.fqdn = fqdn;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getLevel() {
    return level;
  }

  public int getModifiedLevel() {
    return getLevel() * 2;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public List<LineageNode> getWiredTo() {
    return wiredTo;
  }

  public void setWiredTo(List<LineageNode> wiredTo) {
    this.wiredTo = wiredTo;
  }

  public void isWiredTo(LineageNode node) {
    this.wiredTo.add(node);
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public void setFqdn(String fqdn) {
    this.fqdn = fqdn;
  }

  public String getFqdn() {
    return fqdn;
  }

  @Override
  public String toString() {
    return id + " " + label + " " + level;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LineageNode)) {
      return false;
    } else {
      LineageNode node = (LineageNode) obj;
      return label.equals(node.getLabel());
    }
  }

}