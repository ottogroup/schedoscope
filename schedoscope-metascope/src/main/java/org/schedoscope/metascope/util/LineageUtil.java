/*
 * Copyright 2017 Otto (GmbH & Co KG)
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
package org.schedoscope.metascope.util;

import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.util.model.MetascopeSchemaLineage;
import org.schedoscope.metascope.util.model.MetascopeLineageEdge;
import org.schedoscope.metascope.util.model.MetascopeLineageNode;

import java.util.*;

public class LineageUtil {
    public static Set<MetascopeLineageEdge> getViewLineage(MetascopeTable table) {
        Set<MetascopeLineageEdge> result = new HashSet<>();

        getRecursiveViewLineage(result, table, true);
        getRecursiveViewLineage(result, table, false);

        return result;
    }

    private static Set<MetascopeLineageEdge> getRecursiveViewLineage(Set<MetascopeLineageEdge> soFar, MetascopeTable table, boolean isBackward) {
        List<MetascopeTable> otherTables = isBackward ? table.getDependencies() : table.getSuccessors();

        MetascopeLineageNode thisNode = new MetascopeLineageNode(table.getFqdn(), table.getTableName(), table.getDatabaseName());
        for (MetascopeTable otherTable : otherTables) {
            MetascopeLineageNode otherNode = new MetascopeLineageNode(otherTable.getFqdn(), otherTable.getTableName(), otherTable.getDatabaseName());
            MetascopeLineageEdge edge;
            if (isBackward) {
                edge = new MetascopeLineageEdge(otherNode, thisNode);
            } else {
                edge = new MetascopeLineageEdge(thisNode, otherNode);
            }
            if (soFar.add(edge)) {
                soFar.addAll(getRecursiveViewLineage(soFar, otherTable, isBackward));
            }
        }

        return soFar;
    }

    public static MetascopeSchemaLineage getSchemaLineage(MetascopeTable table) {
        MetascopeSchemaLineage schemaLineage = new MetascopeSchemaLineage();

        List<MetascopeLineageEdge> forwardEdges = new ArrayList<>();
        List<MetascopeLineageEdge> backwardEdges = new ArrayList<>();

        for (MetascopeField metascopeField : table.getFields()) {
            forwardEdges.addAll(computeRecursiveSchemaLineage(metascopeField, false, new HashSet<MetascopeField>()));
        }

        for (MetascopeField metascopeField : table.getFields()) {
            backwardEdges.addAll(computeRecursiveSchemaLineage(metascopeField, true, new HashSet<MetascopeField>()));
        }

        schemaLineage.setForwardEdges(forwardEdges);
        schemaLineage.setBackwardEdges(backwardEdges);

        return schemaLineage;
    }

    /**
     * Computes the recursive schema lineage edges for a field
     *
     * @param field      the field to compute the edges for
     * @param isBackward {@code true}: backward, {@code false}: forward
     * @return the edges to display in the graph
     */
    private static Set<MetascopeLineageEdge> computeRecursiveSchemaLineage(MetascopeField field, boolean isBackward, Set<MetascopeField> seen) {
        seen.add(field);
        Collection<MetascopeField> lineage = isBackward ? field.getDependencies() : field.getSuccessors();
        HashSet<MetascopeLineageEdge> result = new HashSet<>();
        if (lineage.isEmpty()) return result;

        for (MetascopeField otherField : lineage) {
            MetascopeLineageNode from = new MetascopeLineageNode(field.getFieldId(), field.getFieldName(), field.getTable().getFqdn());
            MetascopeLineageNode to = new MetascopeLineageNode(otherField.getFieldId(), otherField.getFieldName(), otherField.getTable().getFqdn());
            result.add(new MetascopeLineageEdge(from, to));
            if (!seen.contains(otherField)) {
                result.addAll(computeRecursiveSchemaLineage(otherField, isBackward, seen));
            }
        }

        return result;
    }
}
