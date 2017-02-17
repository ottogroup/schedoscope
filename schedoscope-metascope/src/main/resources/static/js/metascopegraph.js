/**
 * http://usejsdoc.org/
 */

/*
 * Function that draws two Graphs and adds some functionality like, forward and
 * backward trace by clicking on a node. The two graphs are not the same but
 * Linked trough some globals (gNodes, gEdges, selectedNode)
 *
 *
 * @param graphData, is the json data holding nodes and edges graphOptions, an
 * object of option parameters lying in graphconfig.js smallGraphOptions,
 * options for the small graph bigGraphContainer, the container in which the big
 * graph is being drawn smallGraphContainer, the container in which the small
 * graph is being drawn
 */
metaScopeLineageGraph.doGraph = function(graphData, graphOptions,
        smallGraphOptions, bigGraphContainer, smallGraphContainer, selectedNode) {
	var fqdnValue = selectedNode;
    // global SelectedNode to pass this between the two graphs
	var initState = true;
    var datajson = graphData;
    var container = bigGraphContainer;
    var options = graphOptions;
    var gNodes = datajson["nodes"];
    var gEdges = datajson["edges"];
    var theNode = gNodes.get({
    	filter: function(item){
    		return item.fqdn == fqdnValue;
    	}
    })
    var theSelectedNode = theNode[0].id;
    var smallContainer = smallGraphContainer;
    var smallOptions = smallGraphOptions;
    var allNodesArray = [];
    var allEdgesArray = [];
    var gSteps = 3;

    var allNodes = gNodes.get({
        returnType : "Object"
    });
    var allEdges = gEdges.get({
        returnType : "Object"
    });
    var data = {
        nodes : gNodes,
        edges : gEdges
    };

    var smallNodes = [];
    var smallEdges = [];
    var smallInitData = {
        nodes : smallNodes,
        edges : smallEdges
    };

    var network = new vis.Network(container, data, options);
    var bottomGraph = new vis.Network(smallContainer, smallInitData,
            smallOptions);
    network.storePositions();

    /*
     * a function that reinitalizes the net.
     *
     */
    function mrClean() {
        for ( var nodeId in allNodes) {
           allNodes[nodeId].color = 'black';
        	allNodes[nodeId].icon.color = 'black';
            allNodesArray.push(allNodes[nodeId]);
        }
        for ( var edgeId in allEdges) {
            allEdges[edgeId].color = 'black';
            allEdgesArray.push(allEdges[edgeId]);
        }
        gNodes.update(allNodesArray);
        gEdges.update(allEdgesArray);
        network.fit();
    }

    network.on("deselectNode", function(params) {
        bottomGraph.setData(smallInitData);
    	//$("#lineageTable").empty();
    	//$("#lineageName").empty();
        mrClean();
    });

    network.on("afterDrawing", function(params){
    	if(initState){
    		initState = false;
    		network.selectNodes([theSelectedNode]);
    		displayRoutine(theSelectedNode, gSteps);
            bottomGraph.redraw();
            detailTable(allNodes[theSelectedNode].fqdn, allNodes[theSelectedNode].group);
    	}

    })
    // colors all nodes gray
    function grayNodes() {
        for ( var node in allNodes) {
            allNodes[node].icon.color = '#d9d9d9';
           // allNodes[node].image = '/images/database-gray.png';
            allNodesArray.push(allNodes[node]);
        }
        for ( var edge in allEdges) {
            allEdges[edge].color = '#d9d9d9';
            allEdgesArray.push(allEdges[edge]);
        }
    }

    /*
     * @param startNode:int, fwdJumps:int @return {'nodes': [], 'edges':[]}
     * returns all forward Nodes from given StartNode in given range fwdJumps
     */
    function pushFwdNodes(startNode, fwdJumps) {
        var fwdNodes = [ startNode ];
        var markedTuple = {
            'nodes' : [ startNode ],
            'edges' : []
        };

        for (var i = 0; i < fwdJumps; i++) {
            var currentFwdNodes = fwdNodes;
            fwdNodes = [];
            for ( var node in currentFwdNodes) {
                var nodeEdges = network
                        .getConnectedEdges(currentFwdNodes[node]);
                for ( var edge in nodeEdges) {
                    var edgeObj = allEdges[nodeEdges[edge]];
                    var isForwardNode = edgeObj.from === currentFwdNodes[node];
                    if (isForwardNode) {
                        edgeObj.color = {
                            inherit : 'to'
                        };
                        fwdNodes.push(edgeObj.to);
                        if (!_.contains(markedTuple.nodes, edgeObj.to)) {
                            markedTuple.nodes.push(edgeObj.to);
                        }
                        if (!_.contains(markedTuple.edges, edgeObj.id)) {
                            markedTuple.edges.push(edgeObj.id);
                        }
                    }
                }
            }
        }
        return markedTuple;
    }

    /*
     * @param startNode:int, bwdJumps:int @return {'nodes': [], 'edges':[]}
     * returns all backward Nodes from given StartNode in given range bwdJumps
     */
    function pushBwdNodes(startNode, bwdJumps) {
        var bwdNodes = [ startNode ];
        var markedTuple = {
            'nodes' : [ startNode ],
            'edges' : []
        };
        for (var i = 0; i < bwdJumps; i++) {
            var currentBwdNodes = bwdNodes;
            bwdNodes = [];
            for ( var node in currentBwdNodes) {
                var nodeEdges = network
                        .getConnectedEdges(currentBwdNodes[node]);
                for ( var edge in nodeEdges) {
                    var edgeObj = allEdges[nodeEdges[edge]];
                    var isBackwardNode = edgeObj.to === currentBwdNodes[node];
                    if (isBackwardNode) {
                        edgeObj.color = {
                            inherit : 'from'
                        };
                        bwdNodes.push(edgeObj.from);
                        if (!_.contains(markedTuple.nodes, edgeObj.from)) {
                            markedTuple.nodes.push(edgeObj.from);
                        }
                        if (!_.contains(markedTuple.edges, edgeObj.id)) {
                            markedTuple.edges.push(edgeObj.id);
                        }
                    }
                }
            }
        }
        return markedTuple;
    }

    /*
     * pure facade function, that marks all paths from and to a given startNode
     * of a given length @param startNode: a node ID as int, steps: a Int
     * @return all marked nodes and edges as an Object
     */
    function getNeighborhoodGraph(startNode, steps) {
        var fwdTuple = pushFwdNodes(startNode, steps);
        var bwdTuple = pushBwdNodes(startNode, steps);
        var nodesToMark = fwdTuple.nodes.concat(bwdTuple.nodes);
        var edgesToMark = fwdTuple.edges.concat(bwdTuple.edges);
        var combinedMarkedTuple = {
            'nodes' : [],
            'edges' : []
        };
        combinedMarkedTuple.nodes.push(nodesToMark);
        combinedMarkedTuple.edges.push(edgesToMark);
        return combinedMarkedTuple;
    }

    /*
     * @param nodes, edges: as an Array @return Options Object for a subgraph
     * rename [rawData, rawEdges, rawNodes] = getRawData(nodes, edges) smallData =
     * rawData smallEdges = rawEdges smallNodes = rawNodes
     */
    function smallGraphData(nodes, edges) {
        var rawSmallNodes = new vis.DataSet(gNodes.get(_.uniq(nodes)));
        smallEdges = new vis.DataSet(gEdges.get(edges));
        smallNodes = rawSmallNodes.get({
            order : "level"
        });

        var smallData = {
            nodes : smallNodes,
            edges : smallEdges
        };
        return smallData;
    }

    function markNodes(nodeArray) {
        for ( var node in nodeArray) {
           // allNodes[nodeArray[node]].image = '/images/database-red.png';
            allNodes[nodeArray[node]].icon.color = 'black';
        }
    }
    /*
     * ties all the functions together that will visualize the path in the
     * graph.
     */
    function displayRoutine(theSelectedNode, steps) {
        grayNodes();
        var markedTuple = getNeighborhoodGraph(theSelectedNode, steps);
        markNodes(markedTuple.nodes[0]);
        //allNodes[theSelectedNode].image = '/images/database-orange.png';
        allNodes[theSelectedNode].icon.color = 'orange';
        gNodes.update(allNodesArray);
        gEdges.update(allEdgesArray);
        var smallData = smallGraphData(markedTuple.nodes[0],
                markedTuple.edges[0]);
        bottomGraph.setData(smallData);
    }

    // on node select handler
    network.on("selectNode", function(params) {
        gSteps = 2;
        theSelectedNode = parseInt(params.nodes[0]);
        displayRoutine(theSelectedNode, gSteps);
        bottomGraph.redraw();
        detailTable(allNodes[theSelectedNode].fqdn, allNodes[theSelectedNode].group);
    });

    /*
     * Changes the Global steps @param negative values will show you less nodes,
     * positive values will show you more nodes
     *
     */
    function zoom(steps) {
        gSteps = steps + gSteps;
        if (gSteps <= 0) {
            gSteps = 1;
        }
        displayRoutine(theSelectedNode, gSteps);
        bottomGraph.redraw();
    }

    // side effects not good make good commentar or stuff
    bottomGraph.on("selectNode", function(params) {
        theSelectedNode = parseInt(params.nodes[0]);
        displayRoutine(theSelectedNode, gSteps);
        detailTable(allNodes[theSelectedNode].fqdn, allNodes[theSelectedNode].group);
    });

    // Zoom out so you get MORE nodes
    $("#zoomOut").click(function() {
        zoom(1);
    });

    // Zoom in so you get LESS nodes
    $("#zoomIn").click(function() {
        zoom(-1);
    });


  var detailTable = function(fqdn, type) {
    $.ajax({
      url : '/table/view/lineage/detail',
      type : 'GET',
      data : {
        fqdn : fqdn,
        type : type
      },
      success : setLineageDetail
    });
  }

  var setLineageDetail = function(data) {
    $("#lineageDetail").html(data);
  }

}