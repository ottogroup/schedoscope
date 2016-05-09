/*!
 * metascope
 * https://github.com/ottogroup/schedoscope
 * Copyright 2016 Otto Group and other contributors; Licensed MIT
 */

$(function() {
 

  var url = document.location.toString();
  if (url.match('#')) {
    var urlPart = url.split('#')[1];
    $("#" + urlPart).show();
  } 
});
