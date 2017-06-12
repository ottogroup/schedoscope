package org.schedoscope.metascope.util.model;

public class MetascopeLineageNode {

    private String id;
    private String label;
    private String parent;

    public MetascopeLineageNode(String id, String label, String parent) {
        this.id = id;
        this.label = label;
        this.parent = parent;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public String getParent() {
        return parent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetascopeLineageNode that = (MetascopeLineageNode) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
