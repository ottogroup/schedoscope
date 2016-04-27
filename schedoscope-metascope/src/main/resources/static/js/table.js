/*!
 * metascope
 * https://github.com/ottogroup/schedoscope
 * Copyright 2016 Otto Group and other contributors; Licensed MIT
 */

$(function() {
  $.fn.sort_select_box = function() {
    var my_options = $("#" + this.attr('id') + ' option');
    var selected = $("#" + this.attr('id')).val();
    my_options.sort(function(a, b) {
      if (a.text > b.text)
        return 1;
      else if (a.text < b.text)
        return -1;
      else
        return 0
    })
    $(this).empty().append(my_options);
    $(this).val(selected);
  }

  $('#partitionSelectBox').sort_select_box();

  /* GET lineage information on lineage modal open event */
  $('#lineage').on('shown.bs.modal', function(e) {
    $.ajax({
      url : '/table/view/lineage',
      type : 'GET',
      data : $('#lineageForm').serialize(),
      success : drawLineage
    });
  });

  /* initialize tablesorter plugin */
  initTablesorter();

  var url = document.location.toString();
  if (url.match('#')) {
    var urlPart = url.split('#')[1];
    var s = '.nav-pills a[href=#' + urlPart + ']';
    if (urlPart.contains('-')) {
      s = '.nav-pills a[href=#' + urlPart.split('-')[0] + ']';
    }
    $(s).tab('show');

    var section = urlPart.split('-')[0];
    if (section === 'schemaContent' || section === 'parameterContent') {
      var field = urlPart.split('-')[1];
      initializeFieldEditor(field); 
      showFieldDocu(section + "-" + field);
    }
  }

  /* show appropriate content on hash change */
  $(window).on('hashchange', function(e) {
    var url = document.location.toString();
    var urlPart = url.split('#')[1];
    var s;
    if (typeof urlPart == 'undefined') {
      s = '.nav-pills a[href=#documentationContent]';
      $(s).tab('show');
    } else {
      s = '.nav-pills a[href=#' + urlPart + ']';
      if (urlPart.contains('-')) {
        s = '.nav-pills a[href=#' + urlPart.split('-')[0] + ']';
      }
      $(s).tab('show');
    }
  });

  /* set hash in URL for page reload */
  $('.nav-pills a').on('shown.bs.tab', function(e) {
    window.location.hash = e.target.hash; // TODO comment out for navigation?!
    window.scrollTo(0, 0);
  })

  $('#tags').tagsinput({
    confirmKeys : [ 13, 32, 188 ]
  });
});
