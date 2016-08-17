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

package org.schedoscope.export.writables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

public class TextPairArrayWritable extends ArrayWritable {

    public TextPairArrayWritable() {
        super(TextPairWritable.class);
    }

    public TextPairArrayWritable(TextPairWritable[] data) {
        super(TextPairWritable.class, data);
    }

    public List<Text> getSecondAsList() {
        List<Text> secondValues = new ArrayList<Text>();
        for (Writable v : get()) {
            secondValues.add(((TextPairWritable) v).getSecond());
        }
        return secondValues;
    }

    public List<Text> getFirstAsList() {
        List<Text> firstValues = new ArrayList<Text>();
        for (Writable v : get()) {
            firstValues.add(((TextPairWritable) v).getFirst());
        }
        return firstValues;
    }
}
