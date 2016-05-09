package org.schedoscope.metascope.model;

import java.util.ArrayList;
import java.util.List;

public class CategoryMap {

	private List<CategoryEntity> categories;
	private List<CategoryObjectEntity> categoryObjects;
	
	public CategoryMap() {
		this.categories = new ArrayList<CategoryEntity>();
		this.categoryObjects = new ArrayList<CategoryObjectEntity>();
	}
	
	public List<CategoryEntity> getCategories() {
	  return categories;
  }
	
	public List<CategoryObjectEntity> getCategoryObjects() {
	  return categoryObjects;
  }
	
	public void addToCategories(CategoryEntity categorie) {
		if (!categories.contains(categorie)) {
			categories.add(categorie);
		}
	}
	
	public void addToCategoryObjects(CategoryObjectEntity categoryObject) {
		if (!categoryObjects.contains(categoryObject)) {
			categoryObjects.add(categoryObject);
		}
	}
	
	public String getCategoriesAsString() {
		String res = "";
		for (CategoryEntity category : categories) {
	    if (!res.isEmpty()) {
	    	res += ",";
	    }
	    res += category.getName();
    }
		return res;
	}
	
	public String getCategoryObjectsAsString() {
		String res = "";
		for (CategoryObjectEntity categoryObject : categoryObjects) {
	    if (!res.isEmpty()) {
	    	res += ",";
	    }
	    res += categoryObject.getName();
    }
		return res;
	}
	
}
