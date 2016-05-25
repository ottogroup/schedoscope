/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

$(function() {
  /* set the cookies for the header / get filter collapse status */
  setupFilter();

  $("#searchInputField").on("input", function(e) {
    $("#searchInputField").autocomplete({
      source : function(request, response) {
        $.ajax({
          url : '/solr/suggest',
          type : 'GET',
          data : {
            userInput : $("#searchInputField").val(),
          },
          success : function(data) {
            response(JSON.parse(data))
          }
        });
      },
      select : function(event, ui) {
        preSubmitFilterForm();
        filterForm.submit();
      }
    });
    jQuery.ui.autocomplete.prototype._resizeMenu = function() {
      var ul = this.menu.element;
      ul.outerWidth(this.element.outerWidth());
    }
  });

});