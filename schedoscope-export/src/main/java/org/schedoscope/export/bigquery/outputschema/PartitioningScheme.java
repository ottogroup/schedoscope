package org.schedoscope.export.bigquery.outputschema;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;

public class PartitioningScheme {

    public enum Granularity {
        DAILY, MONTHLY
    }

    private Optional<String> temporalPartitionColumn = empty();

    private Optional<Granularity> granularity = empty();

    private List<String> logicalPartitionColumns = new LinkedList<>();


    public Optional<String> getTemporalPartitionColumn() {
        return temporalPartitionColumn;
    }

    public Optional<Granularity> getGranularity() {
        return granularity;
    }

    public List<String> getLogicalPartitionColumns() {
        return logicalPartitionColumns;
    }

    public boolean isTemporallyPartitioned() {
        return getTemporalPartitionColumn().isPresent() && getGranularity().isPresent();
    }

    public boolean isLogicallyPartitioned() {
        return !logicalPartitionColumns.isEmpty();
    }

    public PartitioningScheme(String temporalPartitionColumn, Granularity granularity, String... logicalPartitionColumns) {
        this.granularity = Optional.of(granularity);
        this.temporalPartitionColumn = Optional.of(temporalPartitionColumn);
        this.logicalPartitionColumns.addAll(Arrays.asList(logicalPartitionColumns));
    }

    public PartitioningScheme() {
    }

}
