package edu.unika.aifb.keywordsearch.api;

import it.unimi.dsi.util.BloomFilter;

import java.util.Collection;
import java.util.Set;

import edu.unika.aifb.keywordsearch.KeywordElement;

public interface IEntity  extends IResource {
	
	public void setType(INamedConcept type);
	
	public INamedConcept getType();
	
	public void setExtension(String extension);
	
	public String getExtension();
	
	public void setReachaleEntities(BloomFilter entities);
	
	public BloomFilter getReachaleEntities();
	
	public boolean isReachable(IEntity entity);
	
	public boolean isReachable(KeywordElement ele);
	
	public boolean isReachable(Collection<KeywordElement> elements);
	
	public boolean isAllReachable(Collection<Collection<KeywordElement>> colls);
	
	public void addAttributeValueCompound(IAttributeValueCompound compound);
	
	public void setAttributeValueCompounds(Set<IAttributeValueCompound> compounds);
	
	public Set<IAttributeValueCompound> getAttributeValueCompounds();
	
	public void addAttributeValue(IValue value);
	
	public void setAttributeValues(Set<IValue> values);
	
	public Set<IValue> getAttributeValues();
	
}
