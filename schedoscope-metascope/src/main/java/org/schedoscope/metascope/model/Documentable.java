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

import javax.persistence.FetchType;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import java.util.List;

@MappedSuperclass
public abstract class Documentable {

  @OneToOne(fetch = FetchType.EAGER)
  private MetascopeComment comment;
  @OneToMany(fetch = FetchType.EAGER)
  private List<MetascopeComment> comments;

  public MetascopeComment getComment() {
    return comment;
  }

  public void setComment(MetascopeComment comment) {
    this.comment = comment;
  }

  public List<MetascopeComment> getComments() {
    return comments;
  }

  public void setComments(List<MetascopeComment> comments) {
    this.comments = comments;
  }

  public boolean hasDocumentation() {
    return comment != null;
  }

  public boolean hasComments() {
    return comments.size() > 0;
  }

}
