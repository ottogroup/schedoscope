/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ 

$(function() {
  /** AJAX AND FORM SETUP **/

  /* send csrf tokens with each AJAX request */
  $(document).ajaxSend(function(e, xhr, options) {
    var token = $("input[name='_csrf']").val();
    var header = "X-CSRF-TOKEN";
    xhr.setRequestHeader(header, token);
  });
  
  /* register pre-submit handler for the filter/search form */
  $("#filterForm").submit(preSubmitFilterForm);

  /** CSS SETUP **/
  $('#minimizeButton').on('click', hideExpandIdentity);
  $("#elementsSelect").css("width", "inherit");

  /* make table header collapsable */
  $('#expandComments').on('click', expandFunction);
});