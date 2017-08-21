package org.schedoscope.metascope.task.metastore.model;

public class MetastoreTable {

    public MetastoreTable(String owner, long createTime, String inputFormat, String outputFormat, String location, String schedoscopeTimestamp) {
        this.owner = owner;
        this.createTime = createTime;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.location = location;
        this.schedoscopeTimestamp = schedoscopeTimestamp;
    }

    private String owner;
    private long createTime;
    private String inputFormat;
    private String outputFormat;
    private String location;
    private String schedoscopeTimestamp;

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getSchedoscopeTimestamp() {
        return schedoscopeTimestamp;
    }

    public void setSchedoscopeTimestamp(String schedoscopeTimestamp) {
        this.schedoscopeTimestamp = schedoscopeTimestamp;
    }

}
