package org.schedoscope.metascope.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

@Entity
public class CategoryEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long categoryId;

	@ManyToOne
	private TaxonomyEntity taxonomy;

	private String name;

	@OneToMany(mappedBy = "category", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
	private List<CategoryObjectEntity> categoryObjects;

	public CategoryEntity() {
		this.categoryObjects = new ArrayList<CategoryObjectEntity>();
	}

	public long getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(long categoryId) {
		this.categoryId = categoryId;
	}

	public TaxonomyEntity getTaxonomy() {
		return taxonomy;
	}

	public void setTaxonomy(TaxonomyEntity taxonomy) {
		this.taxonomy = taxonomy;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<CategoryObjectEntity> getCategoryObjects() {
		return categoryObjects;
	}

	public void setCategoryObjects(List<CategoryObjectEntity> categoryObjects) {
		this.categoryObjects = categoryObjects;
	}

}
