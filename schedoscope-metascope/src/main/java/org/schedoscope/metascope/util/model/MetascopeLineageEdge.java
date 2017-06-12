package org.schedoscope.metascope.util.model;

public class MetascopeLineageEdge {

    private MetascopeLineageNode from;
    private MetascopeLineageNode to;

    public MetascopeLineageEdge(MetascopeLineageNode from, MetascopeLineageNode to) {
        this.from = from;
        this.to = to;
    }

    public void setFrom(MetascopeLineageNode from) {
        this.from = from;
    }

    public void setTo(MetascopeLineageNode to) {
        this.to = to;
    }

    public MetascopeLineageNode getFrom() {
        return from;
    }

    public MetascopeLineageNode getTo() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetascopeLineageEdge that = (MetascopeLineageEdge) o;

        return (from != null ? from.equals(that.from) : that.from == null) && (to != null ? to.equals(that.to) : that.to == null);
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        return result;
    }
}
