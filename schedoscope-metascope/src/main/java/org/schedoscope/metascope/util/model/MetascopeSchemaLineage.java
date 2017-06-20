package org.schedoscope.metascope.util.model;

import java.util.List;

public class MetascopeSchemaLineage {

    private List forwardEdges;
    private List backwardEdges;

    public void setForwardEdges(List forwardEdges) {
        this.forwardEdges = forwardEdges;
    }

    public void setBackwardEdges(List backwardEdges) {
        this.backwardEdges = backwardEdges;
    }

    public List getForwardEdges() {
        return forwardEdges;
    }

    public List getBackwardEdges() {
        return backwardEdges;
    }

}
