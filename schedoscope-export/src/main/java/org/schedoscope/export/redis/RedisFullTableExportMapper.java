/**
 * Copyright 2016 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.redis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.redis.outputformat.RedisHashWritable;
import org.schedoscope.export.redis.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.CustomHCatListSerializer;
import org.schedoscope.export.utils.StatCounter;

/**
 * A mapper to read a full Hive table via HCatalog and emits a RedisWritable containing
 * all columns and values as pairs.
 */
public class RedisFullTableExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, RedisHashWritable> {

    private Configuration conf;

    private HCatSchema schema;

    private String keyName;

    private String keyPrefix;

    private CustomHCatListSerializer serializer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();
        schema = HCatInputFormat.getTableSchema(conf);

        serializer = new CustomHCatListSerializer(conf, schema);
//        jsonMapper = new ObjectMapper();
//
//        StringBuilder columnNameProperty = new StringBuilder();
//        StringBuilder columnTypeProperty = new StringBuilder();
//
//        String prefix = "";
//        serde = new JsonSerDe();
//        for (HCatFieldSchema f : schema.getFields()) {
//            columnTypeProperty.append(prefix);
//            columnNameProperty.append(prefix);
//            prefix = ",";
//            columnNameProperty.append(f.getName());
//            columnTypeProperty.append(f.getTypeString());
//
//        }
//
//        Properties tblProps = new Properties();
//        tblProps.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperty.toString());
//        tblProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypeProperty.toString());
//        try {
//            serde.initialize(conf, tblProps);
//            inspector = serde.getObjectInspector();
//        } catch (SerDeException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        RedisOutputFormat.checkKeyType(schema, conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME));

        keyName = conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME);
        keyPrefix = RedisOutputFormat.getExportKeyPrefix(conf);
    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value, Context context)
            throws IOException, InterruptedException {

        Text redisKey = new Text(keyPrefix + value.getString(keyName, schema));

        MapWritable redisValue = new MapWritable();
        boolean write = false;

        for (String f : schema.getFieldNames()) {

            Object obj = value.get(f, schema);
            if (obj != null) {
                String jsonString = obj.toString();

                if (schema.get(f).isComplex()) {
                    try {
                        jsonString = serializer.getJsonComplexType(value, f);
                    } catch (Exception e) {

                    }
                }
                redisValue.put(new Text(f), new Text(jsonString));
                write = true;
            }
        }

        if (write) {
            context.getCounter(StatCounter.SUCCESS).increment(1);
            context.write(redisKey, new RedisHashWritable(redisKey, redisValue));
        } else {
            context.getCounter(StatCounter.FAILED).increment(1);
        }
    }
}
