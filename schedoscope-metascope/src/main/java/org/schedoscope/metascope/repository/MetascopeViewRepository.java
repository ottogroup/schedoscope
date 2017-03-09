package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.MetascopeView;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * Created by kas on 22.11.16.
 */
public interface MetascopeViewRepository extends CrudRepository<MetascopeView, String> {

    @Query("SELECT count(*) FROM MetascopeView v WHERE v.table.fqdn = :fqdn AND v.viewId < :viewId")
    public int getPartitionPosition(@Param(value = "fqdn") String fqdn, @Param(value = "viewId") String viewId);

    @Query("SELECT v.parameterString FROM MetascopeView v WHERE v.table.fqdn = :fqdn ORDER BY v.parameterString")
    public List<String> findParameterStringsForTable(@Param(value = "fqdn") String fqdn);

    public Page<MetascopeView> findByTableFqdnOrderByViewId(String fqdn, Pageable pageable);

    public MetascopeView findFirstByTableFqdn(String fqdn);

}
