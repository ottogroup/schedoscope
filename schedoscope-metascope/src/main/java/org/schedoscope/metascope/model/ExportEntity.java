package org.schedoscope.metascope.model;

import java.util.Map;

import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class ExportEntity {

	public static final String EXPORT_ID = "export_id";
	public static final String FQDN = "fqdn";
	public static final String TYPE = "type";

	@Id
	private String exportId;
	private String fqdn;
	private String type;
	@ElementCollection
	private Map<String, String> properties;

	public String getExportId() {
		return exportId;
	}

	public void setExportId(String exportId) {
		this.exportId = exportId;
	}

	public String getFqdn() {
		return fqdn;
	}

	public void setFqdn(String fqdn) {
		this.fqdn = fqdn;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

}
