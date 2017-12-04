package org.schedoscope.export.bigquery.outputschema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.schedoscope.export.bigquery.outputschema.HCatSchemaTransformer.transformSchema;

public class HCatRecordToBigQueryMapConvertor {

    static private final Log LOG = LogFactory.getLog(HCatRecordToBigQueryMapConvertor.class);

    static private final ObjectMapper jsonConvertor = new ObjectMapper();

    private static class Constructor implements HCatSchemaTransformer.Constructor<HCatRecord, Object, Pair<String, Object>, Map<String, Object>> {

        @Override
        public Function<HCatRecord, Object> accessPrimitiveField(HCatSchema schema, HCatFieldSchema field) {
            return r -> {
                try {
                    return r.get(field.getName(), schema);
                } catch (HCatException e) {
                    // not going to happen
                    return null;
                }
            };
        }

        @Override
        public Function<HCatRecord, Object> accessMapField(HCatSchema schema, HCatFieldSchema field) {
            return r -> {
                try {
                    return r.getMap(field.getName(), schema);
                } catch (HCatException e) {
                    // not going to happen
                    return null;
                }
            };
        }

        @Override
        public Function<HCatRecord, HCatRecord> accessStructField(HCatSchema schema, HCatFieldSchema field) {
            return r -> {
                try {
                    return new DefaultHCatRecord((List<Object>) r.getStruct(field.getName(), schema));
                } catch (HCatException e) {
                    // not going to happen
                    return null;
                }
            };
        }

        @Override
        public Function<HCatRecord, List<Object>> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field) {
            return r -> {
                try {
                    return (List<Object>) r.getList(field.getName(), schema);
                } catch (HCatException e) {
                    // not going to happen
                    return null;
                }
            };
        }

        @Override
        public Function<HCatRecord, List<Object>> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field) {
            return accessPrimitiveArrayField(schema, field);
        }

        @Override
        public Function<HCatRecord, List<Object>> accessMapArrayField(HCatSchema schema, HCatFieldSchema field) {
            return accessPrimitiveArrayField(schema, field);
        }

        @Override
        public Function<HCatRecord, List<HCatRecord>> accessStructArrayField(HCatSchema schema, HCatFieldSchema field) {
            return r -> accessPrimitiveArrayField(schema, field)
                    .apply(r)
                    .stream()
                    .map(s -> new DefaultHCatRecord((List<Object>) s))
                    .collect(Collectors.toList());
        }

        @Override
        public Function<List<Pair<String, Object>>, Map<String, Object>> constructSchema() {
            return ps -> {

                Map<String, Object> m = new HashMap<>();

                for (Pair<String, Object> p : ps)
                    m.put(p.getKey(), p.getValue());

                return m;

            };
        }

        @Override
        public Function<Object, Pair<String, Object>> constructPrimitiveField(HCatFieldSchema field) {
            return o -> new ImmutablePair<>(field.getName(), o);
        }

        @Override
        public Function<Object, Pair<String, Object>> constructMapField(HCatFieldSchema field) {
            return o -> {
                try {
                    return new ImmutablePair<>(field.getName(), jsonConvertor.writeValueAsString(o));
                } catch (JsonProcessingException e) {
                    // should not happen
                    return null;
                }
            };
        }

        @Override
        public Function<Map<String, Object>, Pair<String, Object>> constructStructField(HCatSchema schema, HCatFieldSchema field) {
            return o -> new ImmutablePair<>(field.getName(), o);
        }

        @Override
        public Function<List<Object>, Pair<String, Object>> constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType) {
            return os -> new ImmutablePair<>(field.getName(), os);
        }

        @Override
        public Function<List<Object>, Pair<String, Object>> constructMapArrayField(HCatFieldSchema field) {
            return ms -> new ImmutablePair<>(field.getName(),
                    ms.stream()
                            .map(m -> {
                                try {
                                    return jsonConvertor.writeValueAsString(m);
                                } catch (JsonProcessingException e) {
                                    // should not happen
                                    return null;
                                }
                            })
                            .collect(Collectors.toList())
            );
        }

        @Override
        public Function<List<Object>, Pair<String, Object>> constructArrayArrayField(HCatFieldSchema field) {
            return as -> new ImmutablePair<>(field.getName(),
                    as.stream()
                            .map(a -> {
                                try {
                                    return jsonConvertor.writeValueAsString(a);
                                } catch (JsonProcessingException e) {
                                    // should not happen
                                    return null;
                                }
                            })
                            .collect(Collectors.toList())
            );
        }

        @Override
        public Function<List<Map<String, Object>>, Pair<String, Object>> constructStructArrayField(HCatSchema schema, HCatFieldSchema field) {
            return ss -> new ImmutablePair<>(field.getName(), ss);
        }
    }


    private static final Constructor c = new Constructor();

    static public Map<String, Object> convertHCatRecordToBigQueryMap(HCatSchema schema, HCatRecord record) throws JsonProcessingException {

        try {
            LOG.info("Incoming HCat record: " + record.toString() + " of Schema: " + schema.toString());

            Map<String, Object> bigQueryMap = transformSchema(c, schema).apply(record);

            LOG.info("Outgoing BigQuery map: " + jsonConvertor.writeValueAsString(bigQueryMap));

            return bigQueryMap;

        } catch (JsonProcessingException e) {
            // should not happen
            LOG.error("Error converting HCatRecord", e);

            throw e;
        }

    }
}
