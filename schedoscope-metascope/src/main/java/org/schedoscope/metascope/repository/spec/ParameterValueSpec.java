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
package org.schedoscope.metascope.repository.spec;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.schedoscope.metascope.model.ParameterValueEntity;
import org.springframework.data.jpa.domain.Specification;

public class ParameterValueSpec {

  public static Specification<ParameterValueEntity> queryWithParams(final String fqdn, final String urlPathPrefix,
      final String next) {
    return new Specification<ParameterValueEntity>() {
      @Override
      public Predicate toPredicate(Root<ParameterValueEntity> root, CriteriaQuery<?> query, CriteriaBuilder builder) {
        List<Predicate> predicates = new ArrayList<Predicate>();
        predicates.add(builder.equal(root.get("tableFqdn"), fqdn));
        predicates.add(builder.equal(root.get("key").get("pKey"), next));
        predicates.add(builder.like(root.get("key").<String> get("urlPath"), urlPathPrefix + "%"));
        return builder.and(predicates.toArray(new Predicate[] {}));
      }
    };
  }
}
