package org.schedoscope.metascope.task.metastore.model;

import java.util.ArrayList;
import java.util.List;

public class MetastorePartition {

    private List<String> values;
    private String numRows;
    private String totalSize;
    private String schedoscopeTimestamp;

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
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

    public void setValuesFromName(String parameterString) {
        List<String> parameters = new ArrayList<>();
        if (parameterString != null && !parameterString.isEmpty()) {
            String[] params = parameterString.split("/");
            for (int i = 1; i < params.length; i++) {
                String[] kv = params[i].split("=");
                parameters.add(kv[1]);
            }
        }
        this.values = parameters;
    }

}
