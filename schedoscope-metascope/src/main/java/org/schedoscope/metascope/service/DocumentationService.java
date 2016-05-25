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

import org.jsoup.Jsoup;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.Documentable;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.UserEntity;
import org.schedoscope.metascope.repository.CommentEntityRepository;
import org.schedoscope.metascope.repository.FieldEntityRepository;
import org.schedoscope.metascope.repository.TableEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DocumentationService {

	private static final Logger LOG = LoggerFactory
			.getLogger(DocumentationService.class);

	@Autowired
	private UserEntityService userEntityService;
	@Autowired
	private ActivityEntityService activityEntityService;
	@Autowired
	private CommentEntityRepository commentEntityRepository;
	@Autowired
	private TableEntityRepository tableEntityRepository;
	@Autowired
	private FieldEntityRepository fieldEntityRepository;
	@Autowired
	private SolrFacade solr;

	public CommentEntity findById(String commentID) {
		return commentEntityRepository.findOne(Long.parseLong(commentID));
	}

	@Transactional
	public void updateDocumentation(Documentable documentable,
			String documentText, UserEntity userEntity) {
		if (documentable == null) {
			return;
		}

		if (documentText != null && !documentText.isEmpty()) {
			CommentEntity comment = documentable.getComment();
			if (comment == null) {
				comment = new CommentEntity();
				documentable.setComment(comment);
			}

			comment.setText(documentText);
			comment.setPlainText(Jsoup.parse(documentText).body().text());
			comment.setUsername(userEntity.getUsername());
			comment.setLastEdit(System.currentTimeMillis());
			commentEntityRepository.save(comment);
			documentable.setComment(comment);
		}

		saveEntity(documentable, userEntity);

		if (documentable instanceof TableEntity) {
			activityEntityService.createUpdateDocumentActivity(
					((TableEntity) documentable), userEntityService.getUser()
							.getUsername());
		} else if (documentable instanceof FieldEntity) {
			TableEntity tableEntity = tableEntityRepository
					.findByFqdn(((FieldEntity) documentable).getFqdn());
			activityEntityService.createUpdateDocumentActivity(tableEntity,
					userEntityService.getUser().getUsername());
		}
	}

	@Transactional
	public void addComment(Documentable documentable, String commentText,
			UserEntity userEntity) {
		if (documentable == null) {
			return;
		}

		if (commentText != null && !commentText.isEmpty()) {
			CommentEntity comment = new CommentEntity();
			comment.setText(commentText);
			comment.setPlainText(Jsoup.parse(commentText).body().text());
			comment.setUsername(userEntity.getUsername());
			comment.setLastEdit(System.currentTimeMillis());
			commentEntityRepository.save(comment);
			documentable.getComments().add(comment);
		}
		saveEntity(documentable, userEntity);

		if (documentable instanceof TableEntity) {
			activityEntityService.createNewCommentActivity(
					(TableEntity) documentable, userEntityService.getUser()
							.getUsername());
		} else if (documentable instanceof FieldEntity) {
			TableEntity tableEntity = tableEntityRepository
					.findByFqdn(((FieldEntity) documentable).getFqdn());
			activityEntityService.createNewCommentActivity(tableEntity,
					userEntityService.getUser().getUsername());
		}
	}

	public void editComment(CommentEntity commentEntity, String commentText,
			UserEntity userEntity) {
		if (commentEntity == null) {
			return;
		}

		if (commentText != null && !commentText.isEmpty()) {
			commentEntity.setText(commentText);
			commentEntity.setPlainText(Jsoup.parse(commentText).body().text());
			commentEntity.setUsername(userEntity.getUsername());
			commentEntity.setLastEdit(System.currentTimeMillis());
			commentEntityRepository.save(commentEntity);
		}
	}

	public void deleteComment(Documentable documentable,
			CommentEntity commentEntity, UserEntity userEntity) {
		if (documentable == null) {
			return;
		}

		if (commentEntity != null) {
			documentable.getComments().remove(commentEntity);
			saveEntity(documentable, userEntity);
			commentEntityRepository.delete(commentEntity);
		}
	}

	private void saveEntity(Documentable documentable, UserEntity userEntity) {
		if (documentable instanceof TableEntity) {
			TableEntity tableEntity = (TableEntity) documentable;
			tableEntityRepository.save(tableEntity);
			LOG.info("User '{}' modified comment for table '{}'",
					userEntity.getUsername(), tableEntity.getFqdn());
			solr.updateTableEntityAsync(tableEntity, true);
		} else if (documentable instanceof FieldEntity) {
			FieldEntity fieldEntity = (FieldEntity) documentable;
			fieldEntityRepository.save(fieldEntity);
			LOG.info("User '{}' modified comment for field '{}' ({})",
					userEntityService.getUser().getUsername(),
					fieldEntity.getName(), fieldEntity.getFqdn());
		}
	}

}
