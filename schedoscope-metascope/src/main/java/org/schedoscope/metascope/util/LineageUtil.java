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
package org.schedoscope.metascope.util;

import org.schedoscope.metascope.model.TableDependencyEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.service.TableEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class LineageUtil {

    @Autowired
    @Lazy
    private TableEntityService service;
    @Autowired
    private HTMLUtil util;

    public String getLineage(TableEntity tableEntity) {
        Set<TableEntity> lineageRoots = getLineageRoots(tableEntity, new HashSet<TableEntity>());
        ArrayList<LineageNode> visited = new ArrayList<LineageNode>();
        List<LineageNode> graph = new ArrayList<LineageNode>();
        for (TableEntity t : lineageRoots) {
            graph.addAll(wireDependencies(t, null, 0, visited));
        }
        return util.convertLineageGraphToVisJsNetwork(graph);
    }

    private Set<TableEntity> getLineageRoots(TableEntity tableEntity, Set<TableEntity> visited) {
        Set<TableEntity> rec = new HashSet<TableEntity>();
        if (!visited.contains(tableEntity)) {
            visited.add(tableEntity);
            List<TableDependencyEntity> successors = service.getSuccessors(tableEntity);
            if (successors == null || successors.size() == 0) {
                rec.add(tableEntity);
            } else {
                for (TableDependencyEntity s : successors) {
                    rec.addAll(getLineageRoots(service.findByFqdn(s.getFqdn()), visited));
                }
            }
        }
        return rec;
    }

    private List<LineageNode> wireDependencies(TableEntity tableEntity, LineageNode node, int level,
                                               List<LineageNode> visited) {
        List<LineageNode> nodes = new ArrayList<LineageNode>();
        LineageNode tableNode;
        String fqdn = tableEntity.getFqdn();
        if (node != null) {
            tableNode = node;
            level++;
        } else {
            tableNode = new LineageNode(fqdn, level++, "tables", fqdn);
        }
        LineageNode transformationNode = new LineageNode(tableEntity.getTransformationType(), level++, "transformations",
                fqdn);
        tableNode.isWiredTo(transformationNode);
        nodes.add(tableNode);
        nodes.add(transformationNode);
        for (TableDependencyEntity dependencyEntity : tableEntity.getDependencies()) {
            LineageNode dependencyNode = getNode(dependencyEntity, visited);
            if (dependencyNode == null) {
                fqdn = dependencyEntity.getDependencyFqdn();
                dependencyNode = new LineageNode(fqdn, level, "tables", fqdn);
            }
            transformationNode.isWiredTo(dependencyNode);
            if (!visited.contains(dependencyNode)) {
                visited.add(dependencyNode);
                nodes.addAll(wireDependencies(service.findByFqdn(dependencyEntity.getDependencyFqdn()), dependencyNode, level,
                        visited));
            }
        }
        return nodes;
    }

    private LineageNode getNode(TableDependencyEntity dependencyEntity, List<LineageNode> visited) {
        for (LineageNode lineageNode : visited) {
            if (lineageNode.getLabel().equals(dependencyEntity.getDependencyFqdn())) {
                return lineageNode;
            }
        }
        return null;
    }

}
