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
package org.schedoscope.metascope.index.model;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SolrQueryResultEntity implements Comparable<SolrQueryResultEntity> {

    private Object resultEntity;
    private Map<String, List<String>> highlightings;

    public SolrQueryResultEntity(Object resultEntity, Map<String, List<String>> map) {
        this.resultEntity = resultEntity;
        this.highlightings = new HashMap<String, List<String>>();
        for (Entry<String, List<String>> e : map.entrySet()) {
            highlightings.put(e.getKey(), e.getValue());
        }
    }

    public SolrQueryResultEntity(Object resultEntity) {
        this.resultEntity = resultEntity;
    }

    public Object getResultEntity() {
        return resultEntity;
    }

    public void setResultEntity(Object resultEntity) {
        this.resultEntity = resultEntity;
    }

    public Map<String, List<String>> getHighlightings() {
        return highlightings;
    }

    public void setHighlightings(Map<String, List<String>> highlightings) {
        this.highlightings = highlightings;
    }

    public int getSize() {
        int size = 0;
        for (Entry<String, List<String>> e : highlightings.entrySet()) {
            if (excludedFromCount(e.getKey())) {
                for (String match : e.getValue()) {
                    size += StringUtils.countMatches(match, "<b>");
                }
            }
        }
        return size;
    }

    private boolean excludedFromCount(String key) {
        return !key.equals("type") && !key.equals("id");
    }

    @Override
    public int compareTo(SolrQueryResultEntity other) {
        if (this.highlightings == null || other.highlightings == null) {
            return 0;
        }
        return Integer.compare(this.getSize(), other.getSize()) * -1;
    }

}
