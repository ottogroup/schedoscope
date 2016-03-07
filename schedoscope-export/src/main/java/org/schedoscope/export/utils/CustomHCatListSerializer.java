package org.schedoscope.export.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomHCatListSerializer {

    private ObjectMapper jsonMapper;

    private JsonSerDe serde;

    private ObjectInspector inspector;

    public CustomHCatListSerializer(Configuration conf, HCatSchema schema) {

        jsonMapper = new ObjectMapper();

        StringBuilder columnNameProperty = new StringBuilder();
        StringBuilder columnTypeProperty = new StringBuilder();

        String prefix = "";
        serde = new JsonSerDe();
        for (HCatFieldSchema f : schema.getFields()) {
            columnTypeProperty.append(prefix);
            columnNameProperty.append(prefix);
            prefix = ",";
            columnNameProperty.append(f.getName());
            columnTypeProperty.append(f.getTypeString());

        }

        Properties tblProps = new Properties();
        tblProps.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperty.toString());
        tblProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypeProperty.toString());
        try {
            serde.initialize(conf, tblProps);
            inspector = serde.getObjectInspector();
        } catch (SerDeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String getJsonComplexType(HCatRecord value, String fieldName) throws IOException, SerDeException {
        return jsonMapper.readTree(serde.serialize(value, inspector).toString()).get(fieldName).toString();
    }

}
