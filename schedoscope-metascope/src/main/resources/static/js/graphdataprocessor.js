/*
 * This function takes the raw data to processes it for the Graph.
 * It sets the layout level and deletes edges that are not necessary
 * The first step is to identify the start nodes and give them level 0.
 * Now we traverse trough the graph to set the levels of the other nodes.
 * When all the nodes are leveled, we detect transitive edges and delete them.
 * At last we return the cleared and leveled data for the Graph.
 *
 *
 * @param raw jsonData in given format data { tables: [], dependency: [] }
 * @return a vis.dataset
 *
 */
metaScopeLineageGraph.processData = function(jsonData) {
	var dataGroups = [];
    // Set start Level of all Nodes to 0 and mark all as not critical
    jsonData.tables.forEach(function(node) {
        node.level = 0;
        node.critical = false;
        node.icon ={};
        node.group = 'table';
        node.fqdn = node.label;
        node.color = 'black';
        node.icon.color = '#000000';
        var text = node.label;
    	var split = text.split(".");
    	var dataname = split[0];
    	dataGroups.push(dataname);
    	var tablename = split[1];
        	dataname = dataname.concat('\n');
        	dataname = dataname.concat(tablename);
        node.label = dataname;
        var timestamp = Math.random()*10
    });
    var gNodes = new vis.DataSet(jsonData.tables);
    var gEdges = new vis.DataSet(jsonData.dependency);
    dataGroups = _.uniq(dataGroups);

    /*
     * @param data(level is set to 0 and critical is set to false for all nodes )
     * @return Leveled data.
     */
    function levelingData(data) {

        // @param: json data @return: array of startnode ids
        function findStartNodes(jsonData) {
            var nodes = jsonData.tables;
            var edges = jsonData.dependency;
            var allNodeIds = [];
            var notStartNodes = [];
            for (var i = 0, l = edges.length; i < l; i++) {
                notStartNodes.push(edges[i].to);
            }
            for (var x = 0, le = nodes.length; x < le; x++) {
                allNodeIds.push(nodes[x].id);
            }
            var startNodes = _.difference(allNodeIds, notStartNodes);
            return startNodes;
        }

        var queue = findStartNodes(data);

        // @param: a nodeid as Int, @return an array of the next forward Nodes
        // with level
        function nextFwdNodes(aNodeId) {
            var connectedEdges = getFwdConnetedEdges(aNodeId);
            var nextNodes = [];
            for ( var edge in connectedEdges) {
                var edgeObj = connectedEdges[edge];
                var isFwdEdge = edgeObj.from === aNodeId;
                if (isFwdEdge) {
                    var previousNode = gNodes.get(edgeObj.from);
                    var currentNode = gNodes.get(edgeObj.to);
                    if (currentNode.level > previousNode.level + 1) {
                        nextNodes.push(edgeObj.to);
                    } else {
                        currentNode.level = previousNode.level + 1;
                        gNodes.update(currentNode);
                        nextNodes.push(edgeObj.to);
                    }
                }
            }
            return nextNodes;
        }

        // Main loop to process the queue
        function processQueue(data) {
            if (data.length === 0) {

            } else {
                var currentNode = data.pop();
                var nextNodes = nextFwdNodes(currentNode);
                nextNodes.forEach(function(e) {
                    data.push(e);
                });
                processQueue(queue);
            }
        }

        processQueue(queue);

        var processedData = {
            nodes : gNodes,
            edges : gEdges
        };
        // return processedData;

    }


    levelingData(jsonData);// level the data.

    // @param : a nodeid as int, @return: all connectedEdges to the given node
    // //getFwdConnetedEdges
    function getFwdConnetedEdges(aNodeId) {
        var connectedEdges = gEdges.get({
            filter : function(item) {
                return (item.from === aNodeId);
            }
        });
        return connectedEdges;
    }

    /*
     * Detect if a edge connects two nodes where the level difference is more
     * then one and mark them as critical, those are candidates for getting
     * deleted. changes are directly done in the gNodes and gEdges.
     */
    function detectLevelGaps() {
        var edgeIds = gEdges.getIds();
        for (var i = 0, l = gNodes.length; i < l; i++) {
            var connectedEdges = getFwdConnetedEdges(gNodes.get(i).id);
            for (var edge = 0, totalEdges = gEdges.length; edge < totalEdges; edge++) {
                var fromNode = gEdges.get(edgeIds[edge]).from;
                var toNode = gEdges.get(edgeIds[edge]).to;
                var criticalEdge = gEdges.get(edgeIds[edge]);
                if (gNodes.get(fromNode).level + 1 < gNodes.get(toNode).level
                        && criticalEdge.critical !== true) {
                    var criticalNode = gNodes.get(fromNode);
                    criticalEdge.critical = true;
                    gNodes.update(criticalNode);
                    gEdges.update(criticalEdge);
                }
            }
        }
    }
    detectLevelGaps();

    // Get edges that are marked as Critical
    function getCriticalEdges() {
        var criticalEdges = gEdges.get({
            filter : function(item) {
                return item.critical;
            }
        });
        return criticalEdges;
    }

    // @param : a nodeid as int @return visted nodes as a Array
    // reachableNodes rename
    function fwdPath(nodeId) {
        var nextNodes = [ nodeId ];
        var vistedNodes = [ nodeId ];
        while (nextNodes.length !== 0) {
            var currentNode = nextNodes.pop();
            var connectedEdges = getFwdConnetedEdges(currentNode);
            for ( var edge in connectedEdges) {
                var currentEdge = connectedEdges[edge];
                var isFwdNode = currentEdge.from === currentNode;
                if (isFwdNode) {
                    nextNodes.push(currentEdge.to);
                    vistedNodes.push(currentEdge.to);
                }
            }
        }
        return vistedNodes;
    }

    // checks if there is a path between two nodes
    // isReachable rename
    function alternativePath(startNodeId, goalNodeId) {
        var vistedNodes = fwdPath(startNodeId);
        return _.contains(vistedNodes, goalNodeId);
    }

    // checks if a given critical edge is obsolete or not, returns true or
    // false
    function isEdgeObsolete(edgeObj) {
        var startNode = edgeObj.from;
        var goalNode = edgeObj.to;
        var connectedEdges = getFwdConnetedEdges(startNode);
        for ( var edges in connectedEdges) {
            var currentEdge = connectedEdges[edges];
            if (currentEdge.id !== edgeObj.id) {
                if (currentEdge.from === startNode) {
                    if (alternativePath(currentEdge.to, goalNode)) {
                        return true;
                    }
                }
            }
        }
    }

    /*
     * @param Array of edge objects. checks each edge in the given array if the
     * edge is obsolete, if so they will be deleted in gEdges.
     */
    function deleteEdges(edgeArray) {
        edgeArray.forEach(function(edge) {
            if (isEdgeObsolete(edge)) {
                var currentEdge = gEdges.get(edge.id);
                gEdges.remove(currentEdge.id);
            }
        });
    }

    var criticalEdges = getCriticalEdges();

    deleteEdges(criticalEdges);

    var finalData = {
        nodes : gNodes,
        edges : gEdges
    };

    return finalData;
}
