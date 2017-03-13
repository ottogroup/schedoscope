package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.service.MetascopeStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeStatusController {

    @Autowired
    private MetascopeStatusService metascopeStatusService;

    /**
     * Retrieves the current schedoscope status for a table
     *
     * @param request
     * @param qualifier the fully qualified domain name for entity to get status from
     * @return
     */
    @RequestMapping(value = "/status/", method = RequestMethod.GET)
    @ResponseBody
    public String getStatus(HttpServletRequest request, String qualifier) {
        return metascopeStatusService.getStatus(qualifier);
    }

}
