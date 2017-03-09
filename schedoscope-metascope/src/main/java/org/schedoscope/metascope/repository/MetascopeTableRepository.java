package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.MetascopeCategoryObject;
import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeTable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

/**
 * Created by kas on 22.11.16.
 */
public interface MetascopeTableRepository extends CrudRepository<MetascopeTable, String> {

  @Query("SELECT t FROM MetascopeTable t WHERE :co MEMBER OF t.categoryObjects")
  public List<MetascopeTable> findByCategoryObject(@Param(value = "co") MetascopeCategoryObject co);

  @Query("SELECT t FROM MetascopeTable t WHERE :commentEntity MEMBER OF t.comments")
  public MetascopeTable findByComment(@Param(value = "commentEntity") MetascopeComment commentEntity);

  @Query("SELECT distinct(t.personResponsible) FROM MetascopeTable t where t.personResponsible is not null")
  public Set<String> getAllOwner();

  public List<MetascopeTable> findTop5ByOrderByViewCountDesc();

  @Query("SELECT t.fqdn FROM MetascopeTable t")
  public List<String> getAllTablesNames();

}
