package org.schedoscope.metascope.util;

import org.schedoscope.metascope.util.model.Node;
import org.schedoscope.metascope.model.MetascopeTable;

import java.util.*;

/**
 * Created by kas on 28.11.16.
 */
public class LineageUtil {

    public static String getDependencyGraph(MetascopeTable table) {
        List<Node> topLevelNodes = getTopLevelNodes(table, 0);
        Collections.sort(topLevelNodes);
        Map<String, Node> visited = new HashMap<>();
        for (Node topLevelNode : topLevelNodes) {
            createDependencyGraph(topLevelNode, visited);
        }

        int i = 0;
        int minLevel = Integer.MAX_VALUE;
        int maxLevel = Integer.MIN_VALUE;
        for (Node node : visited.values()) {
            node.setId(i++);
            if (node.getDistanceToCentralNode() < minLevel) {
                minLevel = node.getDistanceToCentralNode();
            }
            if (node.getDistanceToCentralNode() > maxLevel) {
                maxLevel = node.getDistanceToCentralNode();
            }
        }

        return convertLineageGraphToVisJsNetwork(visited.values(), maxLevel, minLevel);
    }

    private static void createDependencyGraph(Node parent, Map<String, Node> visited) {
        if (visited.get(parent.getTable().getFqdn()) == null) {
            visited.put(parent.getTable().getFqdn(), parent);
            for (MetascopeTable metascopeTable : parent.getTable().getDependencies()) {
                Node n = visited.get(metascopeTable.getFqdn());
                boolean alreadyVisited = true;
                if (n == null) {
                    n = new Node();
                    n.setTable(metascopeTable);
                    n.setDistanceToCentralNode(parent.getDistanceToCentralNode() - 1);
                    alreadyVisited = false;
                }
                n.addToNexts(parent);
                parent.addToPrevious(n);
                if (!alreadyVisited) {
                    createDependencyGraph(n, visited);
                }
            }
        }
    }

    private static List<Node> getTopLevelNodes(MetascopeTable table, int distance) {
        List<Node> toplevelNodes = new ArrayList<>();
        if (table.getSuccessors().isEmpty()) {
            Node node = new Node();
            node.setTable(table);
            node.setDistanceToCentralNode(distance);
            toplevelNodes.add(node);
        } else {
            for (MetascopeTable metascopeTable : table.getSuccessors()) {
                if (metascopeTable.getFqdn().equals(table.getFqdn())) {
                    continue;
                }

                List<Node> nodes = getTopLevelNodes(metascopeTable, distance++);
                if (toplevelNodes.isEmpty()) {
                    toplevelNodes.addAll(nodes);
                } else {
                    List<Node> newNodes = new ArrayList<>();
                    for (Node newNode : nodes) {
                        boolean isNew = true;
                        for (Node existing : toplevelNodes) {
                            if (existing.getTable().equals(newNode.getTable())) {
                                isNew = false;
                                if (existing.getDistanceToCentralNode() < newNode.getDistanceToCentralNode()) {
                                    existing.setDistanceToCentralNode(newNode.getDistanceToCentralNode());
                                }
                            }
                        }
                        if (isNew) {
                            newNodes.add(newNode);
                        }
                    }
                    toplevelNodes.addAll(newNodes);
                }
            }
        }
        return toplevelNodes;
    }

    public static String convertLineageGraphToVisJsNetwork(Collection<Node> graph, int maxLevel, int minLevel) {
        for (Node node : graph) {
            node.setLevel(node.getDistanceToCentralNode() - minLevel);
        }
        String nodes = "{\n \"nodes\": [\n";
        String edges = "\n \"edges\": [\n";
        int nodeCounter = 0;
        int edgeCounter = 0;
        for (Node n : graph) {
            if (nodeCounter > 0) {
                nodes += ", ";
            }
            String tableName = n.getTable().getFqdn().replace(".", "\\n");
            nodes += "{\n \"id\": " + n.getId() + ",\n \"label\": \"" + tableName + "\",\n \"group\": \"tables\""
                    + ",\n \"level\": " + n.getLevel() * 2 + ",\n \"fqdn\": \"" + n.getTable().getFqdn() + "\"\n}";
            nodeCounter++;
            for (Node d : n.getNexts()) {
                if (edgeCounter > 0) {
                    edges += ", ";
                }
                edges += "{\n \"from\": " + d.getId() + ",\n \"to\": " + n.getId() + ",\n \"arrows\": \"from\"\n}";
                edgeCounter++;
            }
        }
        nodes += "],";
        edges += "]}";
        return nodes + edges;
    }

}
