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

import org.schedoscope.metascope.model.TableDependencyEntity;

@Embeddable
public class TableDependencyEntityKey implements Serializable {

	private static final long serialVersionUID = 4318941563721216802L;

	@Column(name = TableDependencyEntity.FQDN)
	private String fqdn;
	@Column(name = TableDependencyEntity.DEPENDENCY_FQDN)
	private String dependencyFqdn;

	public TableDependencyEntityKey() {
	}

	public TableDependencyEntityKey(String fqdn, String dependencyFqdn) {
		this.fqdn = fqdn;
		this.dependencyFqdn = dependencyFqdn;
	}

	public String getFqdn() {
		return fqdn;
	}

	public void setFqdn(String fqdn) {
		this.fqdn = fqdn;
	}

	public String getDependencyFqdn() {
		return dependencyFqdn;
	}

	public void setDependencyFqdn(String depdencyFqdn) {
		this.dependencyFqdn = depdencyFqdn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dependencyFqdn == null) ? 0 : dependencyFqdn.hashCode());
		result = prime * result + ((fqdn == null) ? 0 : fqdn.hashCode());
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
		TableDependencyEntityKey other = (TableDependencyEntityKey) obj;
		if (dependencyFqdn == null) {
			if (other.dependencyFqdn != null)
				return false;
		} else if (!dependencyFqdn.equals(other.dependencyFqdn))
			return false;
		if (fqdn == null) {
			if (other.fqdn != null)
				return false;
		} else if (!fqdn.equals(other.fqdn))
			return false;
		return true;
	}

}