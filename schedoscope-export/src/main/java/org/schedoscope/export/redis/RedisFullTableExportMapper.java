/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.export.redis;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.redis.outputformat.RedisHashWritable;
import org.schedoscope.export.redis.outputformat.RedisOutputFormat;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;
import org.schedoscope.export.utils.HCatUtils;
import org.schedoscope.export.utils.StatCounter;

import java.io.IOException;
import java.util.Set;

/**
 * A mapper to read a full Hive table via HCatalog and emits a RedisWritable
 * containing all columns and values as pairs.
 */
public class RedisFullTableExportMapper extends
        Mapper<WritableComparable<?>, HCatRecord, Text, RedisHashWritable> {

    private Configuration conf;

    private HCatSchema schema;

    private String keyName;

    private String keyPrefix;

    private HCatRecordJsonSerializer serializer;

    private Set<String> anonFields;

    private String salt;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();
        schema = HCatInputFormat.getTableSchema(conf);

        serializer = new HCatRecordJsonSerializer(conf, schema);

        HCatUtils.checkKeyType(schema,
                conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME));

        keyName = conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME);
        keyPrefix = RedisOutputFormat.getExportKeyPrefix(conf);

        anonFields = ImmutableSet.copyOf(conf.getStrings(
                BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));
        salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");
    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value,
                       Context context) throws IOException, InterruptedException {

        Text redisKey = new Text(keyPrefix + value.getString(keyName, schema));

        MapWritable redisValue = new MapWritable();
        boolean write = false;

        for (String f : schema.getFieldNames()) {

            Object obj = value.get(f, schema);
            if (obj != null) {
                String jsonString;

                if (schema.get(f).isComplex()) {
                    jsonString = serializer.getFieldAsJson(value, f);
                } else {
                    jsonString = obj.toString();
                    jsonString = HCatUtils.getHashValueIfInList(f, jsonString,
                            anonFields, salt);
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
