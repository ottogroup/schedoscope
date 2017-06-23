"use strict";

/**
 * Represents a graph to show the lineage of a view's fields.
 * @param {string} containerSelector A W3C selector string for the container to draw into
 * @param {string} direction Direction for rank nodes. Can be TB, BT, LR or RL, where T = top, B = bottom, L = left and R = right.
 * @constructor
 */
function LineageGraph(containerSelector, direction) {
    const CLUSTER_LABEL_POS = "top";
    const SVG_NAMESPACE_URI = "http://www.w3.org/2000/svg";

    var container = document.querySelector(containerSelector);
    if (container === null)
        throw "The selector '" + containerSelector + "' does not match any element.";

    var svg = document.createElementNS(SVG_NAMESPACE_URI, "svg");
    if (direction === "RL") svg.style.float = "right";
    container.appendChild(svg);
    var d3svg = d3.select(svg);
    var render = new dagreD3.render();
    var parents = {};
    var fullGraph = new dagreD3.graphlib.Graph()
        .setGraph({})
        .setDefaultEdgeLabel(function () {
            return {};
        })
        .setDefaultNodeLabel(function () {
            return {};
        });
    var drawnGraph = new dagreD3.graphlib.Graph({compound: true})
        .setGraph({
            rankdir: direction,
            transition: function (selection) {
                return selection.transition().duration(256);
            }
        })
        .setDefaultEdgeLabel(function () {
            return {};
        })
        .setDefaultNodeLabel(function (nodeId) {
            return fullGraph.node(nodeId);
        });

    var redraw = function () {
        render(d3svg, drawnGraph);

        svg.setAttribute("width", drawnGraph.graph().width);
        svg.setAttribute("height", drawnGraph.graph().height);

        d3svg.selectAll(".node").on("click", this.addNeighbours);
        d3svg.selectAll(".cluster").insert("title")
            .text(function (d) {
                return d.split(".")[0]
            });
    }.bind(this);

    var addStartNode = function (id) {
        // add the node
        drawnGraph.setNode(id);
        drawnGraph.node(id).class += " start";

        // add the parent node
        addParentOf(id);
    };

    /**
     * Adds an edge and its parent to the drawn graph.
     * @param {Object} edge The edge to add
     */
    var addEdge = function (edge) {
        drawnGraph.setEdge(edge.v, edge.w);
        addParentOf(edge.v);
        addParentOf(edge.w);
    };

    var addParentOf = function (node) {
        var parent = parents[node];
        var delimiter = parent.includes(".") ? "." : "_";
        var splitParent = parent.split(delimiter);
        drawnGraph.setNode(parent, {label: splitParent[splitParent.length - 1], clusterLabelPos: CLUSTER_LABEL_POS});
        drawnGraph.setParent(node, parent);
    };

    /**
     * Adds all neighbor nodes of the given node id to the graph.
     *
     * @param {String} nodeId a node id
     */
    this.addNeighbours = function (nodeId) {
        if (!fullGraph.hasNode(nodeId))
            throw "Unknown node id " + nodeId;

        var neighbourEdges = fullGraph.outEdges(nodeId).concat(fullGraph.inEdges(nodeId));
        var newEdges = neighbourEdges.filter(function (edge) {
            return !drawnGraph.hasEdge(edge);
        });
        if (newEdges.length === 0) return;

        newEdges.forEach(addEdge);
        redraw();
        d3svg.selectAll(".cluster .label").each(function () {
            var label = d3.select(this);
            var g = label.select(g);

            label.append("a")
                .attr("href", function (d) {
                    return "?fqdn=" + d;
                })
                .node().appendChild(this.querySelector("g"));
        });

        d3svg.selectAll(".node").classed("disabled", function (d) {
            var toAdd = fullGraph.outEdges(d).concat(fullGraph.inEdges(d));
            return toAdd.every(function (edge) {
                return drawnGraph.hasEdge(edge);
            });
        });
    };

    /**
     * Sets the data to display in the graph. All sources will be display initially.
     *
     * @param {Object[]} edgesData All edges to display.
     * @param {String[]} startNodes (Optional) The nodes to display at the beginning. If not given, all graph sources will be shown.
     */
    this.setData = function (edgesData, startNodes) {
        if (edgesData.length === 0) return;

        edgesData.forEach(function (edgeData) {
            fullGraph.setEdge(edgeData.from.id, edgeData.to.id);
            [edgeData.from, edgeData.to].forEach(function (node) {
                fullGraph.setNode(node.id, {label: node.label, class: "btn btn-default"});
                parents[node.id] = node.parent;
            });
        });

        // check if graph is acyclic
        if (!dagreD3.graphlib.alg.isAcyclic(fullGraph)) {
            var cycles = dagreD3.graphlib.alg.findCycles(fullGraph);
            var message = "Lineage graph is not acyclic. Brace yourself, display bugs are coming!";
            cycles.forEach(function (cycle) {
                message += "\nFound cycle: " + cycle.join(" → ") + " → " + cycle[0];
            });
            alert(message);
        }

        if (!startNodes) {
            startNodes = fullGraph.sources();
        }
        startNodes.forEach(addStartNode);
        redraw();
    };
}