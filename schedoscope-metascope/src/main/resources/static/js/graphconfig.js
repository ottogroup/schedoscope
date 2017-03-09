/**
 * http://usejsdoc.org/
 */
var metaScopeLineageGraph = {};

metaScopeLineageGraph.bigOptions = {
    nodes : {
    	font: {
  	      color: '#343434',
  	      size: 14, // px
  	      face: 'FontAwesome',
  	 },
        scaling : {
            min : 2,
            max : 2,
            label: {
                enabled: false
            }

        },
        icon: {
            face: 'FontAwesome',
            code: '\uf0ce',
            size: 50,  //50,
            color:'#000000'
          },
          shape: 'icon'

    },
    interaction : {
        tooltipDelay : 200
    },
    edges : {
    	//arrowStrikethrough: false,
        arrows : 'to',
        color : {
            inherit : 'to'
        }
    },
    layout : {
        hierarchical : {
            enabled : true,
            direction : 'LR',
            sortMethod : 'hubsize',
            levelSeparation : 600
        }
    },
   // font: {
   //     align: 'left'
//      },
    physics : false,
    autoResize : true,
    height : '400px',
    width : '100%'
};

metaScopeLineageGraph.smallOptions = {
    layout : {
        hierarchical : {
            enabled : true,
            direction : 'LR',
            sortMethod : 'directed'
        }
    },
    nodes : {
    	 font: {
    	      color: '#343434',
    	      size: 14, // px
    	      face: 'FontAwesome',
    	 },
        scaling : {
            min : 2,
            max : 2,
            label: {
                enabled: false
            }
        },
        fixed : true,
        icon: {
            face: 'FontAwesome',
            code: '\uf0ce',
            size: 50,  //50,
            color:'green'
          },
          shape: 'icon'

    },
    edges : {
    	//arrowStrikethrough: false,
        arrows : {

            to : {
                enabled : true,
                scaleFactor : 1
            }
        },
        color : {
            inherit : 'to'
        },
    },
    physics : {
        enabled : true,
    },
   // font: {
   //     align: 'left'
   //   },
    height : '400px',
    width : '100%'
};
