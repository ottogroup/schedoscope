package org.schedoscope.metascope.model;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class MetascopeAutoSave {

    @Id
    private String id;
    private String tableFqdn;
    @Column(columnDefinition = "varchar(32629)")
    private String text;
    private long timestamp;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getTableFqdn() {
        return tableFqdn;
    }

    public void setTableFqdn(String tableFqdn) {
        this.tableFqdn = tableFqdn;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
