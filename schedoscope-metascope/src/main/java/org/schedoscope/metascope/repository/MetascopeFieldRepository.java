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

import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface MetascopeFieldRepository extends CrudRepository<MetascopeField, String> {

    @Query("SELECT f.fieldName, MAX(f.fieldOrder) FROM MetascopeField f where f.isParameter = true GROUP BY f.fieldName ORDER BY MAX(f.fieldOrder)")
    public List<Object[]> findDistinctParameters();

    @Query("SELECT f FROM MetascopeField f WHERE :commentEntity MEMBER OF f.comments")
    public MetascopeField findByComment(@Param(value = "commentEntity") MetascopeComment commentEntity);

}
