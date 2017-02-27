package org.schedoscope.metascope.model;

import javax.persistence.*;
import java.util.Map;

/**
 * Created by kas on 23.11.16.
 */
@Entity
public class MetascopeTransformation {

    @Id
    private String transformationId;
    private String transformationType;
    @ElementCollection
    @Column(length = 32000)
    private Map<String, String> properties;
    @OneToOne(fetch = FetchType.LAZY)
    private MetascopeTable table;

    public String getTransformationId() {
        return transformationId;
    }

    public void setTransformationId(String transformationId) {
        this.transformationId = transformationId;
    }

    public String getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(String transformationType) {
        this.transformationType = transformationType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public MetascopeTable getTable() {
        return table;
    }

    public void setTable(MetascopeTable table) {
        this.table = table;
    }

}
