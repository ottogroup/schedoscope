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
package org.schedoscope.metascope.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class CommentEntity {

	@Id
	@GeneratedValue
	private Long id;
	@Column(columnDefinition = "varchar(32629)")
	private String text;
	@Column(columnDefinition = "varchar(32629)")
	private String plainText;
	private String username;
	private long lastEdit;

	public Long getId() {
		return id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getPlainText() {
		return plainText;
	}

	public void setPlainText(String plainText) {
		this.plainText = plainText;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public long getLastEdit() {
		return lastEdit;
	}

	public void setLastEdit(long lastEdit) {
		this.lastEdit = lastEdit;
	}

	public String getPreview() {
		return plainText.length() <= 75 ? plainText : plainText
				.substring(0, 75) + " ...";
	}

}
