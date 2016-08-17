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
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.BaseExportJob;
import org.schedoscope.export.redis.outputformat.*;
import org.schedoscope.export.utils.HCatUtils;
import org.schedoscope.export.utils.StatCounter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A mapper that reads from Hive tables and emits a RedisWritable.
 */
public class RedisExportMapper extends
        Mapper<WritableComparable<?>, HCatRecord, Text, RedisWritable> {

    private Configuration conf;

    private HCatSchema schema;

    private String keyName;

    private String valueName;

    private String keyPrefix;

    private Set<String> anonFields;

    private String salt;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();
        schema = HCatInputFormat.getTableSchema(conf);

        HCatUtils.checkKeyType(schema,
                conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME));
        HCatUtils.checkValueType(schema,
                conf.get(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME));

        keyName = conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME);
        valueName = conf.get(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME);

        keyPrefix = RedisOutputFormat.getExportKeyPrefix(conf);
        anonFields = ImmutableSet.copyOf(conf.getStrings(
                BaseExportJob.EXPORT_ANON_FIELDS, new String[0]));
        salt = conf.get(BaseExportJob.EXPORT_ANON_SALT, "");
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void map(WritableComparable<?> key, HCatRecord value,
                       Context context) throws IOException, InterruptedException {

        Text redisKey = new Text(keyPrefix + value.getString(keyName, schema));
        RedisWritable redisValue = null;
        boolean write = false;

        HCatFieldSchema fieldSchema = schema.get(valueName);

        switch (fieldSchema.getCategory()) {
            case MAP:
                Map<String, String> valMap = (Map<String, String>) value.getMap(
                        valueName, schema);
                if (valMap != null) {
                    redisValue = new RedisHashWritable(redisKey.toString(), valMap);
                    write = true;
                }
                break;
            case ARRAY:
                List<String> valArray = (List<String>) value.getList(valueName,
                        schema);
                if (valArray != null) {
                    redisValue = new RedisListWritable(redisKey.toString(),
                            valArray);
                    write = true;
                }
                break;
            case PRIMITIVE:
                Object obj = value.get(valueName, schema);
                if (obj != null) {
                    String valStr = obj.toString();
                    valStr = HCatUtils.getHashValueIfInList(valueName, valStr,
                            anonFields, salt);
                    redisValue = new RedisStringWritable(redisKey.toString(),
                            valStr);
                    write = true;
                }
                break;
            case STRUCT:
                List<String> valStruct = (List<String>) value.getStruct(valueName,
                        schema);
                HCatSchema structSchema = fieldSchema.getStructSubSchema();
                if (valStruct != null) {
                    MapWritable structValue = new MapWritable();

                    for (int i = 0; i < structSchema.size(); i++) {
                        if (valStruct.get(i) != null) {
                            structValue.put(
                                    new Text(structSchema.get(i).getName()),
                                    new Text(valStruct.get(i)));
                            write = true;
                        }
                    }
                    redisValue = new RedisHashWritable(redisKey, structValue);
                }
                break;
            default:
                break;
        }

        if (write) {
            context.write(redisKey, redisValue);
            context.getCounter(StatCounter.SUCCESS).increment(1);
        } else {
            context.getCounter(StatCounter.FAILED).increment(1);
        }
    }
}
