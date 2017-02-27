package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeTableRepository;
import org.schedoscope.metascope.service.MetascopeDataDistributionService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

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
