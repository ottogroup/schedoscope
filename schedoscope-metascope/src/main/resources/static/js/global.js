/*!
 * metascope
 * https://github.com/ottogroup/schedoscope
 * Copyright 2016 Otto Group and other contributors; Licensed MIT
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