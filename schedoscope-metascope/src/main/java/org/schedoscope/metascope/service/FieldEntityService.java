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
package org.schedoscope.metascope.service;

import java.util.ArrayList;
import java.util.List;

import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.repository.FieldEntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FieldEntityService {

	@Autowired
	private FieldEntityRepository fieldEntityRepository;

	public FieldEntity findByFqdnAndName(String fqdn, String name) {
		return fieldEntityRepository.findAllByKeyFqdnAndKeyName(fqdn, name);
	}

	public FieldEntity findByComment(CommentEntity commentEntity) {
		return fieldEntityRepository.findByComment(commentEntity);
	}

	public List<String> findDistinctParameters() {
		List<String> list = new ArrayList<String>();
		List<Object[]> parameters = fieldEntityRepository
				.findDistinctParameters();
		for (Object[] field : parameters) {
			list.add((String) field[0]);
		}
		return list;
	}

}
