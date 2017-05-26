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

package org.schedoscope.export.jdbc.outputformat;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.schedoscope.export.writables.TextPairWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The JDBCWritable can be used to write data from a MR job into a database
 * using a JDBC connection.
 */
public class JdbcOutputWritable implements Writable, DBWritable {

    private static final Log LOG = LogFactory.getLog(JdbcOutputWritable.class);

    private static final String STRING = "string";
    private static final String DOUBLE = "double";
    private static final String FLOAT = "float";
    private static final String BOOLEAN = "boolean";
    private static final String INTEGER = "int";
    private static final String LONG = "long";

    private ArrayWritable value;

    /**
     * Default constructor, initializes the internal writables.
     */
    public JdbcOutputWritable() {

        value = new ArrayWritable(TextPairWritable.class);
    }

    /**
     * Constructor to initialize the internal writable set by the param.
     *
     * @param value The data to initialize the writable with
     */
    public JdbcOutputWritable(List<Pair<String, String>> value) {

        this.value = toArrayWritable(value);
    }

    /**
     * Constructor to initialize the internal writable.
     *
     * @param value Another ArrayWritable to copy from.
     */
    public JdbcOutputWritable(ArrayWritable value) {

        this.value = value;
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {

        List<Pair<String, String>> record = fromArrayWritable(value);
        try {
            for (int i = 0; i < record.size(); i++) {

                String type = record.get(i).getLeft()
                        .toLowerCase(Locale.getDefault());

                if (type.equals(JdbcOutputWritable.STRING)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setString(i + 1, record.get(i).getRight());
                    } else {
                        ps.setNull(i + 1, Types.VARCHAR);
                    }

                } else if (type.equals(JdbcOutputWritable.DOUBLE)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setDouble(i + 1,
                                Double.parseDouble(record.get(i).getRight()));
                    } else {
                        ps.setNull(i + 1, Types.DOUBLE);
                    }
                } else if (type.equals(JdbcOutputWritable.FLOAT)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setDouble(i + 1,
                                Double.parseDouble(record.get(i).getRight()));
                    } else {
                        ps.setNull(i + 1, Types.FLOAT);
                    }

                } else if (type.equals(JdbcOutputWritable.BOOLEAN)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setBoolean(i + 1,
                                Boolean.parseBoolean(record.get(i).getRight()));
                    } else {
                        ps.setNull(i + 1, Types.BOOLEAN);
                    }
                } else if (type.equals(JdbcOutputWritable.INTEGER)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setInt(i + 1,
                                Integer.parseInt(record.get(i).getRight()));
                    } else {
                        ps.setNull(i + 1, Types.INTEGER);
                    }
                } else if (type.equals(JdbcOutputWritable.LONG)) {
                    if (!record.get(i).getRight().equals("NULL")) {
                        ps.setLong(i + 1,
                                Long.parseLong(record.get(i).getRight()));
                    } else {
                        ps.setNull(i + 1, Types.BIGINT);
                    }

                } else {
                    LOG.warn("Unknown column type: "
                            + record.get(i).getLeft()
                            .toLowerCase(Locale.getDefault()));
                    ps.setString(i + 1, record.get(i).getRight());
                }
            }
        } catch (
                NumberFormatException n)

        {
            n.printStackTrace();
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        value.write(out);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        value.readFields(in);
    }

    private ArrayWritable toArrayWritable(List<Pair<String, String>> value) {

        TextPairWritable[] ar = new TextPairWritable[value.size()];
        for (int i = 0; i < value.size(); i++) {
            ar[i] = new TextPairWritable(value.get(i).getLeft(), value.get(i)
                    .getRight());
        }
        return new ArrayWritable(TextPairWritable.class, ar);
    }

    private List<Pair<String, String>> fromArrayWritable(ArrayWritable value) {

        List<Pair<String, String>> listValue = new ArrayList<Pair<String, String>>();
        Writable[] ar = value.get();
        for (Writable w : ar) {
            TextPairWritable tpw = (TextPairWritable) w;
            listValue.add(Pair.of(tpw.getFirst().toString(), tpw.getSecond()
                    .toString()));
        }
        return listValue;
    }
}