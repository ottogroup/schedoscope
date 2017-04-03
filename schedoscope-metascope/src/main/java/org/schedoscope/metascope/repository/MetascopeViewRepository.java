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
package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.MetascopeView;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface MetascopeViewRepository extends CrudRepository<MetascopeView, String> {

    @Query("SELECT count(*) FROM MetascopeView v WHERE v.table.fqdn = :fqdn AND v.viewId < :viewId")
    public int getPartitionPosition(@Param(value = "fqdn") String fqdn, @Param(value = "viewId") String viewId);

    @Query("SELECT v.parameterString FROM MetascopeView v WHERE v.table.fqdn = :fqdn ORDER BY v.parameterString")
    public List<String> findParameterStringsForTable(@Param(value = "fqdn") String fqdn);

    public Page<MetascopeView> findByTableFqdnOrderByViewId(String fqdn, Pageable pageable);

    public MetascopeView findFirstByTableFqdn(String fqdn);

}
