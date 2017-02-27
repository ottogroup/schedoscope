package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.service.MetascopeDataDistributionService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeDataDistributionController {

    @Autowired
    private MetascopeTableService metascopeTableService;

    @Autowired
    private MetascopeDataDistributionService metascopeDataDistributionService;

    @RequestMapping("/datadistribution/start")
    public String calculateDistribution(HttpServletRequest request, String fqdn) {
        if (fqdn != null) {
            MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
            if (table != null) {
                MetascopeDataDistributionService.Status status = metascopeDataDistributionService.checkStatus(table);
                if (status != null && status.equals(MetascopeDataDistributionService.Status.NotAvailable)) {
                    metascopeDataDistributionService.calculateDistribution(table);
                }
            }
        }
        return "redirect:" + request.getHeader("Referer") + "#datadistributionContent";
    }

}
