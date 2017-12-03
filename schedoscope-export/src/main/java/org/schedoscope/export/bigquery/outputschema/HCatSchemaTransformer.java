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

        Function<S, F> accessPrimitiveField(HCatFieldSchema field);

        Function<S, F> accessMapField(HCatFieldSchema field);

        Function<S, S> accessStructField(HCatFieldSchema field);

        Function<S, List<F>> accessPrimitiveArrayField(HCatFieldSchema field);

        Function<S, List<F>> accessArrayArrayField(HCatFieldSchema field);

        Function<S, List<F>> accessMapArrayField(HCatFieldSchema field);

        Function<S, List<S>> accessStructArrayField(HCatFieldSchema field);

        Function<List<FT>, ST> constructSchema();

        Function<F, FT> constructPrimitiveField(HCatFieldSchema field);

        Function<F, FT> constructMapField(HCatFieldSchema field);

        Function<ST, FT> constructStructField(HCatSchema schema, HCatFieldSchema field);

        Function<List<F>, FT> constructPrimitiveArrayField(HCatFieldSchema schema, PrimitiveTypeInfo field);

        Function<List<F>, FT> constructMapArrayField(HCatFieldSchema schema);

        Function<List<F>, FT> constructArrayArrayField(HCatFieldSchema field);

        Function<List<ST>, FT> constructStructArrayField(HCatSchema schema, HCatFieldSchema field);
    }


    static public <S, F, FT, ST> Function<S, ST> transformSchema(Constructor<S, F, FT, ST> c, HCatSchema schema) {

        return s ->
                c.constructSchema().apply(
                        schema
                                .getFields()
                                .stream()
                                .map(field -> transformField(c, field).apply(s))
                                .collect(Collectors.toList())
                );

    }

    static public <S, F, FT, ST> Function<S, FT> transformField(Constructor<S, F, FT, ST> c, HCatFieldSchema field) {

        if (HCatFieldSchema.Category.ARRAY == field.getCategory())

            return transformArrayField(c, field);

        else if (HCatFieldSchema.Category.STRUCT == field.getCategory())

            return transformStructField(c, field);

        else if (HCatFieldSchema.Category.MAP == field.getCategory())

            return transformMapField(c, field);

        else

            return transformPrimitiveField(c, field);

    }

    static public <S, F, FT, ST> Function<S, FT> transformPrimitiveField(Constructor<S, F, FT, ST> c, HCatFieldSchema field) {

        return s -> c.constructPrimitiveField(field).apply(
                c.accessPrimitiveField(field).apply(s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformArrayField(Constructor<S, F, FT, ST> c, HCatFieldSchema field) {

        try {

            HCatFieldSchema elementSchema = field.getArrayElementSchema().get(0);
            PrimitiveTypeInfo elementType = elementSchema.getTypeInfo();

            if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())

                return s -> c.constructPrimitiveArrayField(field, elementType).apply(
                        c.accessPrimitiveArrayField(field).apply(s)
                );

            else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())

                return s -> c.constructMapArrayField(field).apply(
                        c.accessMapArrayField(field).apply(s)
                );

            else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())

                return s -> c.constructArrayArrayField(field).apply(
                        c.accessArrayArrayField(field).apply(s)
                );

            else {

                HCatSchema structSchema = elementSchema.getStructSubSchema();

                return s -> c.constructStructArrayField(structSchema, field).apply(
                        c.accessStructArrayField(field).apply(s)
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

    static public <S, F, FT, ST> Function<S, FT> transformMapField(Constructor<S, F, FT, ST> c, HCatFieldSchema field) {

        return s -> c.constructMapField(field).apply(
                c.accessMapField(field).apply(s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformStructField(Constructor<S, F, FT, ST> c, HCatFieldSchema field) {

        try {

            HCatSchema structSchema = field.getStructSubSchema();

            return s -> c.constructStructField(structSchema, field).apply(
                    transformSchema(c, structSchema).apply(
                            c.accessStructField(field).apply(s)
                    )
            );

        } catch (HCatException e) {
            // not going to happen
            return null;
        }
    }
}
