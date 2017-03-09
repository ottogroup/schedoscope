package org.schedoscope.metascope.model;

import javax.persistence.*;
import java.util.Map;

/**
 * Created by kas on 22.11.16.
 */
@Entity
public class MetascopeExport {

    /* fields */
    @Id
    private String exportId;
    private String exportType;
    @ElementCollection
    private Map<String, String> exportProperties;
    @ManyToOne(fetch = FetchType.LAZY)
    private MetascopeTable table;

    /* getter and setter */
    public String getExportId() {
        return exportId;
    }

    public void setExportId(String exportId) {
        this.exportId = exportId;
    }

    public String getExportType() {
        return exportType;
    }

    public void setExportType(String exportType) {
        this.exportType = exportType;
    }

    public Map<String, String> getProperties() {
        return exportProperties;
    }

    public void setProperties(Map<String, String> properties) {
        this.exportProperties = properties;
    }

    public MetascopeTable getTable() {
        return table;
    }

    public void setTable(MetascopeTable table) {
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetascopeExport that = (MetascopeExport) o;

        return exportId.equals(that.exportId);

    }

    @Override
    public int hashCode() {
        return exportId.hashCode();
    }

}
