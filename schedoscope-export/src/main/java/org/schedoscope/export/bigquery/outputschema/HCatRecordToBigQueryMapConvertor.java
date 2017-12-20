package org.schedoscope.export.bigquery.outputschema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.utils.HCatSchemaToBigQueryTransformer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.schedoscope.export.utils.HCatSchemaToBigQueryTransformer.transformSchema;


/**
 * Convert HCat records to maps for use with BigQuery APIs
 */
public class HCatRecordToBigQueryMapConvertor {

    static private final ObjectMapper jsonConvertor = new ObjectMapper();

    private static final HCatSchemaToBigQueryTransformer.Constructor<HCatRecord, Object, Pair<String, Object>, Map<String, Object>> c = new HCatSchemaToBigQueryTransformer.Constructor<HCatRecord, Object, Pair<String, Object>, Map<String, Object>>() {

        @Override
        public Object accessPrimitiveField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            try {
                return hCatRecord.get(field.getName(), schema);
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public Object accessMapField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            try {
                return hCatRecord.getMap(field.getName(), schema);
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public HCatRecord accessStructField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            try {
                return new DefaultHCatRecord((List<Object>) hCatRecord.getStruct(field.getName(), schema));
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public List<Object> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            try {
                return (List<Object>) hCatRecord.getList(field.getName(), schema);
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public List<Object> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            return accessPrimitiveArrayField(schema, field, hCatRecord);
        }

        @Override
        public List<Object> accessMapArrayField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            return accessPrimitiveArrayField(schema, field, hCatRecord);
        }

        @Override
        public List<HCatRecord> accessStructArrayField(HCatSchema schema, HCatFieldSchema field, HCatRecord hCatRecord) {
            return accessPrimitiveArrayField(schema, field, hCatRecord)
                    .stream()
                    .map(s -> new DefaultHCatRecord((List<Object>) s))
                    .collect(Collectors.toList());
        }

        @Override
        public Map<String, Object> constructSchema(List<Pair<String, Object>> pairs) {
            Map<String, Object> m = new HashMap<>();

            for (Pair<String, Object> p : pairs)
                m.put(p.getKey(), p.getValue());

            return m;
        }

        @Override
        public Pair<String, Object> constructPrimitiveField(HCatFieldSchema field, Object o) {
            return new ImmutablePair<>(field.getName(), o);
        }

        @Override
        public Pair<String, Object> constructMapField(HCatFieldSchema field, Object o) {
            try {
                return new ImmutablePair<>(field.getName(), jsonConvertor.writeValueAsString(o));
            } catch (JsonProcessingException e) {
                // should not happen
                return null;
            }
        }

        @Override
        public Pair<String, Object> constructStructField(HCatSchema schema, HCatFieldSchema field, Map<String, Object> stringObjectMap) {
            return new ImmutablePair<>(field.getName(), stringObjectMap);
        }

        @Override
        public Pair<String, Object> constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType, List<Object> objects) {
            return new ImmutablePair<>(field.getName(), objects);
        }

        @Override
        public Pair<String, Object> constructMapArrayField(HCatFieldSchema field, List<Object> objects) {
            return new ImmutablePair<>(field.getName(),
                    objects.stream()
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
        public Pair<String, Object> constructArrayArrayField(HCatFieldSchema field, List<Object> objects) {
            return new ImmutablePair<>(field.getName(),
                    objects.stream()
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
        public Pair<String, Object> constructStructArrayField(HCatSchema schema, HCatFieldSchema field, List<Map<String, Object>> maps) {
            return new ImmutablePair<>(field.getName(), maps);
        }
    };


    /**
     * Given an HCat schema, convert a record to a map representation for use with the BigQuery API.
     *
     * @param schema the HCatSchema to which records conform
     * @param record the record to transform.
     * @return a nested map representing the record sucht that it can be converted to the JSON format expected by
     * the BigQuery API.
     */
    static public Map<String, Object> convertHCatRecordToBigQueryMap(HCatSchema schema, HCatRecord record) {
        return transformSchema(c, schema, record);
    }
}
