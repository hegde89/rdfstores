package edu.unika.aifb.keywordsearch.impl;

import it.unimi.dsi.util.BloomFilter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.StructureGraphUtil;
import edu.unika.aifb.keywordsearch.api.IAttributeValueCompound;
import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.api.INamedConcept;
import edu.unika.aifb.keywordsearch.api.IValue;

public class Entity implements IEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3294446024824712138L;
	
	private String uri;
	private String extension;
	private INamedConcept type;
	private BloomFilter reachableEntities;
	private Set<IAttributeValueCompound> attributeValueCompounds;
	private Set<IValue> values;
	
	public Entity(String uri) {
		this.uri = uri;
	}
	
	public Entity(String uri, String extension) {
		this.uri = uri;
		this.extension = extension;
	}
	
	public String getLabel() {
		return StructureGraphUtil.getLocalName(uri);
	}

	public String getUri() {
		return uri;
	}
	
	public void setType(INamedConcept type) {
		this.type = type;
	}
	
	public INamedConcept getType() {
		return type;
	}
	
	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	public String getExtension() {
		return extension;
	}
	
	public void setReachaleEntities(BloomFilter entities) {
		reachableEntities = entities;
	}
	
	public BloomFilter getReachaleEntities() {
		return reachableEntities;
	}
	
	public boolean isReachable(IEntity entity) {
		return reachableEntities.contains(entity.getUri());
	}
	
	public boolean isReachable(KeywordElement ele) {
		if(ele.getType() != KeywordElement.ENTITY)
			return false;
		return reachableEntities.contains(ele.getResource().getUri());
	}
	
	public boolean isReachable(Collection<KeywordElement> elements) {
		for(KeywordElement ele : elements) {
			if(isReachable(ele)) 
				return true;
		}
		return false;
	}
	
	public boolean isAllReachable(Collection<Collection<KeywordElement>> colls) {
		for(Collection<KeywordElement> coll : colls) {
			if(!isReachable(coll))
				return false;
		}
		return true;
	}
	
	public void addAttributeValueCompound(IAttributeValueCompound compound) {
		if(attributeValueCompounds == null) {
			attributeValueCompounds = new HashSet<IAttributeValueCompound>();
		}
		attributeValueCompounds.add(compound);	
	}
	
	public void setAttributeValueCompounds(Set<IAttributeValueCompound> compounds) {
		attributeValueCompounds = compounds;
	}
	
	public Set<IAttributeValueCompound> getAttributeValueCompounds() {
		return attributeValueCompounds;
	}
	
	public void addAttributeValue(IValue value) {
		if(values == null) {
			values = new HashSet<IValue>();
		}
		values.add(value);	
	}
	
	public void setAttributeValues(Set<IValue> values) {
		this.values = values;
	}
	
	public Set<IValue> getAttributeValues() {
		return values;
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity other = (Entity) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}

}
