package org.schedoscope.export.jdbc.outputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A writable to store a pair of
 * Text writables.
 */
public class TextPairWritable implements Writable {


    private Text first;

    private Text second;

    /**
     * Default constructor, initializes
     * the internal writables.
     */
    public TextPairWritable() {

        first = new Text();
        second = new Text();
    }

    /**
     * A constructor setting the internal
     * writable
     *
     * @param first The first value
     * @param second The second value.
     */
    public TextPairWritable(Text first, Text second) {

        this.first = first;
        this.second = second;
    }

    /**
     * A constructor setting the internal
     * writables from strings.
     *
     * @param first The first value
     * @param second The second value
     */
    public TextPairWritable(String first, String second) {

        this.first = new Text(first);
        this.second = new Text(second);
    }

    @Override
    public void write(DataOutput out) throws IOException {

        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        first.readFields(in);
        second.readFields(in);
    }

    public Text getFirst() {

        return first;
    }

    public Text getSecond() {

        return second;
    }
}
