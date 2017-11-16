package org.schedoscope.export.bigquery.outputschema;

import java.util.Optional;

import static java.util.Optional.empty;

public class TemporalPartitioningScheme {

    public enum Granularity {
        DAILY, MONTHLY
    }

    public Optional<String> getTemporalColumn() {
        return temporalColumn;
    }

    public Optional<Granularity> getGranularity() {
        return granularity;
    }

    private Optional<String> temporalColumn = empty();

    private Optional<Granularity> granularity = empty();

    public boolean isDefined() {
        return getTemporalColumn().isPresent() && getGranularity().isPresent();
    }

    public TemporalPartitioningScheme(String temporalColumn, Granularity granularity) {
        this.granularity = Optional.of(granularity);
        this.temporalColumn = Optional.of(temporalColumn);
    }

    public TemporalPartitioningScheme() {
    }

}
