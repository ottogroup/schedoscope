/*!
 * metascope
 * https://github.com/ottogroup/schedoscope
 * Copyright 2016 Otto Group and other contributors; Licensed MIT
 */

$(function() {
  /* set the cookies for the header / get filter collapse status*/
  setupFilter();
  
  $("#searchInputField").on("input", function(e) {
	  $("#searchInputField").autocomplete({
	    source: function (request, response) {
	      $.ajax({
	        url : '/solr/suggest',
	        type : 'GET',
	        data : {
	        	userInput: $("#searchInputField").val(),
	        },
	        success : function(data) {
	        	response(JSON.parse(data))
	        }
	      });
	    },
	    select: function(event, ui) {
	    	preSubmitFilterForm();
	    	filterForm.submit();
	    }
	  });
	  jQuery.ui.autocomplete.prototype._resizeMenu = function () {
		  var ul = this.menu.element;
		  ul.outerWidth(this.element.outerWidth());
		}
  });
  
});