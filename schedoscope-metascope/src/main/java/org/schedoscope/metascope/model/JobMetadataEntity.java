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

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

import org.schedoscope.metascope.model.key.JobMetadataEntityKey;

@Entity
public class JobMetadataEntity {

	@EmbeddedId
	private JobMetadataEntityKey key;
	private boolean finished;

	public JobMetadataEntity() {
	}

	public JobMetadataEntity(String urlPath, String fieldName, boolean finished) {
		if (key == null) {
			key = new JobMetadataEntityKey(urlPath, fieldName);
		} else {
			setUrlPath(urlPath);
			setFieldName(fieldName);
		}
		this.finished = finished;
	}

	public String getUrlPath() {
		return this.key.getUrlPath();
	}

	public void setUrlPath(String urlPath) {
		this.key.setUrlPath(urlPath);
	}

	public String getFieldName() {
		return this.key.getFieldName();
	}

	public void setFieldName(String fieldName) {
		this.key.setFieldName(fieldName);
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

}
