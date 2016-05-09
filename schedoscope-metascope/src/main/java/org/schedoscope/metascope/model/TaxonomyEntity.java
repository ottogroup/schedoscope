package org.schedoscope.metascope.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
public class TaxonomyEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long taxonomyId;
	
	private String name;

	@OneToMany(mappedBy = "taxonomy", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
	private List<CategoryEntity> categories;

	public TaxonomyEntity() {
		this.categories = new ArrayList<CategoryEntity>();
	}

	public long getTaxonomyId() {
	  return taxonomyId;
  }
	
	public void setTaxonomyId(long taxonomyId) {
	  this.taxonomyId = taxonomyId;
  }
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<CategoryEntity> getCategories() {
		return categories;
	}

	public void setCategories(List<CategoryEntity> categories) {
		this.categories = categories;
	}

}
