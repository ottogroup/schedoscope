package org.schedoscope.metascope.repository;

import org.schedoscope.metascope.model.TaxonomyEntity;
import org.springframework.data.repository.CrudRepository;

public interface TaxonomyEntityRepository extends CrudRepository<TaxonomyEntity, Long> {

	public TaxonomyEntity findByName(String name);
}
