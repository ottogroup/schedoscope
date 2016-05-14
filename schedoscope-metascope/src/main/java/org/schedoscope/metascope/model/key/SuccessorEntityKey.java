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
package org.schedoscope.metascope.model.key;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.schedoscope.metascope.model.SuccessorEntity;

@Embeddable
public class SuccessorEntityKey implements Serializable {

	private static final long serialVersionUID = 4321941562721116802L;

	@Column(name = SuccessorEntity.URL_PATH)
	private String urlPath;
	@Column(name = SuccessorEntity.SUCCESSOR_URL_PATH)
	private String successorUrlPath;
	@Column(name = SuccessorEntity.SUCCESSOR_FQDN)
	private String successorFqdn;

	public SuccessorEntityKey() {
	}

	public SuccessorEntityKey(String urlPath, String successorUrlPath,
			String successorFqdn) {
		this.urlPath = urlPath;
		this.successorUrlPath = successorUrlPath;
		this.successorFqdn = successorFqdn;
	}

	public String getUrlPath() {
		return urlPath;
	}

	public void setUrlPath(String urlPath) {
		this.urlPath = urlPath;
	}

	public String getSuccessorUrlPath() {
		return successorUrlPath;
	}

	public void setSuccessorUrlPath(String successorUrlPath) {
		this.successorUrlPath = successorUrlPath;
	}

	public String getSuccessorFqdn() {
		return successorFqdn;
	}

	public void setSuccessorFqdn(String depdencyFqdn) {
		this.successorFqdn = depdencyFqdn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((successorFqdn == null) ? 0 : successorFqdn.hashCode());
		result = prime
				* result
				+ ((successorUrlPath == null) ? 0 : successorUrlPath.hashCode());
		result = prime * result + ((urlPath == null) ? 0 : urlPath.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SuccessorEntityKey other = (SuccessorEntityKey) obj;
		if (successorFqdn == null) {
			if (other.successorFqdn != null)
				return false;
		} else if (!successorFqdn.equals(other.successorFqdn))
			return false;
		if (successorUrlPath == null) {
			if (other.successorUrlPath != null)
				return false;
		} else if (!successorUrlPath.equals(other.successorUrlPath))
			return false;
		if (urlPath == null) {
			if (other.urlPath != null)
				return false;
		} else if (!urlPath.equals(other.urlPath))
			return false;
		return true;
	}

}
