package org.schedoscope.metascope.task.metastore.model;

public class MetastorePartition {

    private String name;
    private String numRows;
    private String totalSize;
    private String schedoscopeTimestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNumRows() {
        return numRows;
    }

    public void setNumRows(String numRows) {
        this.numRows = numRows;
    }

    public String getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(String totalSize) {
        this.totalSize = totalSize;
    }

    public String getSchedoscopeTimestamp() {
        return schedoscopeTimestamp;
    }

    public void setSchedoscopeTimestamp(String schedoscopeTimestamp) {
        this.schedoscopeTimestamp = schedoscopeTimestamp;
    }

}
