package org.schedoscope.export.bigquery.outputschema;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class LogicalPartitioningScheme {

    List<String> partitionColumns = new LinkedList();

    public LogicalPartitioningScheme() {}

    public LogicalPartitioningScheme(String... partitionColumns) {
        this.partitionColumns = Arrays.asList(partitionColumns);
    }

}
