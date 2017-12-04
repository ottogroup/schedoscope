package org.schedoscope.export.bigquery.outputschema;


import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HCatSchemaTransformer {


    public interface Constructor<S, F, FT, ST> {

        Function<S, F> accessPrimitiveField(HCatSchema schema, HCatFieldSchema field);

        Function<S, F> accessMapField(HCatSchema schema, HCatFieldSchema field);

        Function<S, S> accessStructField(HCatSchema schema, HCatFieldSchema field);

        Function<S, List<F>> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field);

        Function<S, List<F>> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field);

        Function<S, List<F>> accessMapArrayField(HCatSchema schema, HCatFieldSchema field);

        Function<S, List<S>> accessStructArrayField(HCatSchema schema, HCatFieldSchema field);

        Function<List<FT>, ST> constructSchema();

        Function<F, FT> constructPrimitiveField(HCatFieldSchema field);

        Function<F, FT> constructMapField(HCatFieldSchema field);

        Function<ST, FT> constructStructField(HCatSchema schema, HCatFieldSchema field);

        Function<List<F>, FT> constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType);

        Function<List<F>, FT> constructMapArrayField(HCatFieldSchema field);

        Function<List<F>, FT> constructArrayArrayField(HCatFieldSchema field);

        Function<List<ST>, FT> constructStructArrayField(HCatSchema schema, HCatFieldSchema field);
    }


    static public <S, F, FT, ST> Function<S, ST> transformSchema(Constructor<S, F, FT, ST> c, HCatSchema schema) {

        return s ->
                c.constructSchema().apply(
                        schema
                                .getFields()
                                .stream()
                                .map(field -> transformField(c, schema, field).apply(s))
                                .collect(Collectors.toList())
                );

    }

    static public <S, F, FT, ST> Function<S, FT> transformField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        if (HCatFieldSchema.Category.ARRAY == field.getCategory())

            return transformArrayField(c, schema, field);

        else if (HCatFieldSchema.Category.STRUCT == field.getCategory())

            return transformStructField(c, schema, field);

        else if (HCatFieldSchema.Category.MAP == field.getCategory())

            return transformMapField(c, schema, field);

        else

            return transformPrimitiveField(c, schema, field);

    }

    static public <S, F, FT, ST> Function<S, FT> transformPrimitiveField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        return s -> c.constructPrimitiveField(field).apply(
                c.accessPrimitiveField(schema, field).apply(s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformArrayField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        try {

            HCatFieldSchema elementSchema = field.getArrayElementSchema().get(0);
            PrimitiveTypeInfo elementType = elementSchema.getTypeInfo();

            if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())

                return s -> c.constructPrimitiveArrayField(field, elementType).apply(
                        c.accessPrimitiveArrayField(schema, field).apply(s)
                );

            else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())

                return s -> c.constructMapArrayField(field).apply(
                        c.accessMapArrayField(schema, field).apply(s)
                );

            else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())

                return s -> c.constructArrayArrayField(field).apply(
                        c.accessArrayArrayField(schema, field).apply(s)
                );

            else {

                HCatSchema structSchema = elementSchema.getStructSubSchema();

                return s -> c.constructStructArrayField(structSchema, field).apply(
                        c.accessStructArrayField(schema, field).apply(s)
                                .stream()
                                .map(saf -> transformSchema(c, structSchema).apply(saf))
                                .collect(Collectors.toList())
                );

            }

        } catch (HCatException e) {
            // not going to happen

            return null;
        }

    }

    static public <S, F, FT, ST> Function<S, FT> transformMapField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        return s -> c.constructMapField(field).apply(
                c.accessMapField(schema, field).apply(s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformStructField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        try {

            HCatSchema structSchema = field.getStructSubSchema();

            return s -> c.constructStructField(structSchema, field).apply(
                    transformSchema(c, structSchema).apply(
                            c.accessStructField(schema, field).apply(s)
                    )
            );

        } catch (HCatException e) {
            // not going to happen
            return null;
        }
    }
}
