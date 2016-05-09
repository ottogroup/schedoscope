package org.schedoscope.metascope.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

@Entity
public class CategoryObjectEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private long categoryObjectId;
	
	@ManyToOne
	private CategoryEntity category;
	
	private String name;
	
	private String description;

	public long getCategoryObjectId() {
		return categoryObjectId;
	}

	public void setCategoryObjectId(long categoryObjectId) {
		this.categoryObjectId = categoryObjectId;
	}

	public CategoryEntity getCategory() {
		return category;
	}

	public void setCategory(CategoryEntity category) {
		this.category = category;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}
