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
package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.ViewEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface ViewEntityRepository extends CrudRepository<ViewEntity, String> {

    @Query("SELECT count(*) FROM ViewEntity v WHERE v.fqdn = :fqdn")
    public int getPartitionCountForFqdn(@Param(value = "fqdn") String fqdn);

    @Query("SELECT count(*) FROM ViewEntity v WHERE v.fqdn = :fqdn AND v.internalViewId < :viewId")
    public int getPartitionPosition(@Param(value = "fqdn") String fqdn, @Param(value = "viewId") String viewId);

    public Iterable<ViewEntity> findByStatus(String status);

    public ViewEntity findByUrlPath(String urlPath);

    public Page<ViewEntity> findByFqdnOrderByInternalViewId(String fqdn, Pageable pageable);

    public ViewEntity findFirstByFqdn(String fqdn);

}
