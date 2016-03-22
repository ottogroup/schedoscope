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
import org.schedoscope.export.utils.CustomHCatRecordSerializer;
import org.schedoscope.export.utils.HCatUtils;
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

    private CustomHCatRecordSerializer serializer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();
        schema = HCatInputFormat.getTableSchema(conf);

        serializer = new CustomHCatRecordSerializer(conf, schema);

        HCatUtils.checkKeyType(schema, conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME));

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
                String jsonString;

                if (schema.get(f).isComplex()) {
                    jsonString = serializer.getJsonComplexType(value, f);
                } else {
                    jsonString = obj.toString();
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
