package org.schedoscope.metascope.service;

import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeView;
import org.schedoscope.metascope.repository.MetascopeViewRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by kas on 24.11.16.
 */
@Service
public class MetascopeViewService {

    @Autowired
    private MetascopeViewRepository metascopeViewRepository;

    public MetascopeView findByViewId(String viewId) {
        return metascopeViewRepository.findOne(viewId);
    }

}
