package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * Created by kas on 22.11.16.
 */
public interface MetascopeFieldRepository extends CrudRepository<MetascopeField, String> {

    @Query("SELECT f.fieldName, MAX(f.fieldOrder) FROM MetascopeField f where f.isParameter = true GROUP BY f.fieldName ORDER BY MAX(f.fieldOrder)")
    public List<Object[]> findDistinctParameters();

    @Query("SELECT f FROM MetascopeField f WHERE :commentEntity MEMBER OF f.comments")
    public MetascopeField findByComment(@Param(value = "commentEntity") MetascopeComment commentEntity);

}
