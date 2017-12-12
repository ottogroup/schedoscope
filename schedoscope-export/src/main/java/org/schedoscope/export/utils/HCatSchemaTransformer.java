package org.schedoscope.export.utils;


import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HCatSchemaTransformer {


    public interface Constructor<S, F, FT, ST> {

        F accessPrimitiveField(HCatSchema schema, HCatFieldSchema field, S s);

        F accessMapField(HCatSchema schema, HCatFieldSchema field, S s);

        S accessStructField(HCatSchema schema, HCatFieldSchema field, S s);

        List<F> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        List<F> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        List<F> accessMapArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        List<S> accessStructArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        ST constructSchema(List<FT> fts);

        FT constructPrimitiveField(HCatFieldSchema field, F f);

        FT constructMapField(HCatFieldSchema field, F f);

        FT constructStructField(HCatSchema schema, HCatFieldSchema field, ST st);

        FT constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType, List<F> fs);

        FT constructMapArrayField(HCatFieldSchema field, List<F> fs);

        FT constructArrayArrayField(HCatFieldSchema field, List<F> fs);

        FT constructStructArrayField(HCatSchema schema, HCatFieldSchema field, List<ST> sts);
    }


    static public <S, F, FT, ST> Function<S, ST> transformSchema(Constructor<S, F, FT, ST> c, HCatSchema schema) {

        return s ->
                c.constructSchema(
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

        return s -> c.constructPrimitiveField(field,
                c.accessPrimitiveField(schema, field, s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformArrayField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        try {

            HCatFieldSchema elementSchema = field.getArrayElementSchema().get(0);
            PrimitiveTypeInfo elementType = elementSchema.getTypeInfo();

            if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())

                return s -> c.constructPrimitiveArrayField(
                        field, elementType,
                        c.accessPrimitiveArrayField(schema, field, s)
                );

            else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())

                return s -> c.constructMapArrayField(field,
                        c.accessMapArrayField(schema, field, s)
                );

            else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())

                return s -> c.constructArrayArrayField(field,
                        c.accessArrayArrayField(schema, field, s)
                );

            else {

                HCatSchema structSchema = elementSchema.getStructSubSchema();

                return s -> c.constructStructArrayField(structSchema, field,
                        c.accessStructArrayField(schema, field, s)
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

        return s -> c.constructMapField(field,
                c.accessMapField(schema, field, s)
        );

    }

    static public <S, F, FT, ST> Function<S, FT> transformStructField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field) {

        try {

            HCatSchema structSchema = field.getStructSubSchema();

            return s -> c.constructStructField(
                    structSchema, field,
                    transformSchema(c, structSchema).apply(
                            c.accessStructField(schema, field, s)
                    )
            );

        } catch (HCatException e) {
            // not going to happen
            return null;
        }
    }
}
