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
package org.schedoscope.metascope.repository;

import java.util.List;
import java.util.Set;

import org.schedoscope.metascope.model.BusinessObjectEntity;
import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface TableEntityRepository extends CrudRepository<TableEntity, String>,
    JpaSpecificationExecutor<TableEntity> {

  @Query("SELECT t FROM TableEntity t WHERE :commentEntity MEMBER OF t.comments")
  public TableEntity findByComment(@Param(value = "commentEntity") CommentEntity commentEntity);

  @Query("SELECT t FROM TableEntity t WHERE :bo MEMBER OF t.businessObjects")
  public List<TableEntity> findByBusinessObject(@Param(value = "bo") BusinessObjectEntity bo);

  @Query("SELECT distinct(t.personResponsible) FROM TableEntity t where t.tableOwner is not null")
  public Set<String> getAllOwner();

  public TableEntity findByFqdn(String fqdn);

  public List<TableEntity> findTop5ByOrderByViewCountDesc();

}
