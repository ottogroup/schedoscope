/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.export.utils;


import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Library functions for traversing an HCatSchema and transforming a data structure to something else based on the schema
 * according to the capabilities of BigQuery. As BigQuery is less expressive, the transformer functions already perform
 * the correct downgrades of data structures for you. You just need to provide an implementation of the {@link Constructor}
 * interface with appropriate callbacks.
 */
public class HCatSchemaToBigQueryTransformer {


    /**
     * Inferface of callback functions that need to be implemented for a transformation.
     *
     * @param <S>  Type of the schema / record equivalent of the data structure before transformation
     * @param <F>  Type of the field equivalent of the data structure before transformation
     * @param <FT> Type of the schema / record equivalent of the transformation result
     * @param <ST> Type of the field equivalent of the transformation result
     */
    public interface Constructor<S, F, FT, ST> {

        /**
         * Return a primitive field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        F accessPrimitiveField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return a map field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        F accessMapField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return a struct field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        S accessStructField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return a primitive array field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        List<F> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return an array of array field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        List<F> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return an array of map equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        List<F> accessMapArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Return an array of struct field equivalent of the schema / record equivalent before transformation.
         *
         * @param schema the HCat schema
         * @param field  the field
         * @param s      the schema / record equivalent under transformation
         * @return the field equivalent
         */
        List<S> accessStructArrayField(HCatSchema schema, HCatFieldSchema field, S s);

        /**
         * Construct a schema / record equivalent transform from a list of field equivalent transforms
         *
         * @param fts the field transforms
         * @return the schema / record equivalent transform
         */
        ST constructSchema(List<FT> fts);

        /**
         * Construct a transform from a primitive field equivalent
         *
         * @param field the field
         * @param f     the field equivalent
         * @return the field equivalent transform
         */
        FT constructPrimitiveField(HCatFieldSchema field, F f);

        /**
         * Construct a transform from a map field equivalent
         *
         * @param field the field
         * @param f     the field equivalent
         * @return the field equivalent transform
         */
        FT constructMapField(HCatFieldSchema field, F f);

        /**
         * Construct a transform from a struct field equivalent
         *
         * @param schema the record schema
         * @param field  the field
         * @param st     the record transform representing the struct equivalent
         * @return the field equivalent transform
         */
        FT constructStructField(HCatSchema schema, HCatFieldSchema field, ST st);

        /**
         * Construct a transform from a primitive array field equivalent
         *
         * @param field       the field
         * @param elementType the type of the array elements
         * @param fs          the field equivalents
         * @return the field equivalent transform
         */
        FT constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType, List<F> fs);

        /**
         * Construct a transform from an array of map field equivalent
         *
         * @param field the field
         * @param fs    the map field equivalents
         * @return the field equivalent transform
         */
        FT constructMapArrayField(HCatFieldSchema field, List<F> fs);

        /**
         * Construct a transform from an array of array field equivalent
         *
         * @param field the field
         * @param fs    the array field equivalents
         * @return the field equivalent transform
         */
        FT constructArrayArrayField(HCatFieldSchema field, List<F> fs);

        /**
         * Construct a transform from an array of struct field equivalent
         *
         * @param field the field
         * @param sts   the struct field equivalents
         * @return the field equivalent transform
         */
        FT constructStructArrayField(HCatSchema schema, HCatFieldSchema field, List<ST> sts);
    }


    /**
     * Transform a schema / record equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> ST transformSchema(Constructor<S, F, FT, ST> c, HCatSchema schema, S s) {

        return s == null ?
                null : c.constructSchema(
                schema
                        .getFields()
                        .stream()
                        .map(field -> transformField(c, schema, field, s))
                        .collect(Collectors.toList())
        );

    }

    /**
     * Transform a field equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param field  the field to transform
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> FT transformField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field, S s) {

        if (HCatFieldSchema.Category.ARRAY == field.getCategory())

            return transformArrayField(c, schema, field, s);

        else if (HCatFieldSchema.Category.STRUCT == field.getCategory())

            return transformStructField(c, schema, field, s);

        else if (HCatFieldSchema.Category.MAP == field.getCategory())

            return transformMapField(c, schema, field, s);

        else

            return transformPrimitiveField(c, schema, field, s);

    }

    /**
     * Transform a primitive field equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param field  the field to transform
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> FT transformPrimitiveField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field, S s) {

        return c.constructPrimitiveField(field, c.accessPrimitiveField(schema, field, s));

    }

    /**
     * Transform an array field equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param field  the field to transform
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> FT transformArrayField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field, S s) {

        try {

            HCatFieldSchema elementSchema = field.getArrayElementSchema().get(0);
            PrimitiveTypeInfo elementType = elementSchema.getTypeInfo();

            if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())

                return c.constructPrimitiveArrayField(field, elementType, c.accessPrimitiveArrayField(schema, field, s));

            else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())

                return c.constructMapArrayField(field, c.accessMapArrayField(schema, field, s));

            else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())

                return c.constructArrayArrayField(field, c.accessArrayArrayField(schema, field, s));

            else {

                HCatSchema structSchema = elementSchema.getStructSubSchema();

                List<S> structArrayFieldValue = c.accessStructArrayField(schema, field, s);

                return structArrayFieldValue == null ?
                        null : c.constructStructArrayField(structSchema, field,
                        structArrayFieldValue
                                .stream()
                                .map(saf -> transformSchema(c, structSchema, saf))
                                .collect(Collectors.toList())
                );

            }

        } catch (HCatException e) {
            // not going to happen

            return null;
        }

    }

    /**
     * Transform a map field equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param field  the field to transform
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> FT transformMapField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field, S s) {

        return c.constructMapField(field, c.accessMapField(schema, field, s));

    }

    /**
     * Transform a struct field equivalent for a given use case captured by a set of constructors.
     *
     * @param c      the constructors
     * @param schema the HCat schema to traverse
     * @param field  the field to transform
     * @param s      the schema / record equivalent
     * @param <S>    Type of the schema / record equivalent of the data structure before transformation
     * @param <F>    Type of the field equivalent of the data structure before transformation
     * @param <FT>   Type of the schema / record equivalent of the transformation result
     * @param <ST>   Type of the field equivalent of the transformation result
     * @return the transformation result
     */
    static public <S, F, FT, ST> FT transformStructField(Constructor<S, F, FT, ST> c, HCatSchema schema, HCatFieldSchema field, S s) {

        try {

            HCatSchema structSchema = field.getStructSubSchema();

            return c.constructStructField(structSchema, field, transformSchema(c, structSchema, c.accessStructField(schema, field, s)));

        } catch (HCatException e) {
            // not going to happen
            return null;
        }
    }
}
