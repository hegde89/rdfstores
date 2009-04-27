package edu.unika.aifb.keywordsearch.api;

import java.util.Collection;
import java.util.Set;

import edu.unika.aifb.keywordsearch.KeywordElement;

public interface IEntity  extends IResource {
	
	public void setType(INamedConcept type);
	
	public INamedConcept getType();
	
	public void setReachaleEntities(Set<IEntity> entities);
	
	public void addReachableEntity(IEntity entity);
	
	public Set<IEntity> getReachaleEntities();
	
	public boolean isReachable(IEntity entity);
	
	public boolean isReachable(KeywordElement ele);
	
	public boolean isReachable(Collection<KeywordElement> elements);
	
	public boolean isAllReachable(Collection<Collection<KeywordElement>> colls);
	
	public void setAttributeValueCompounds(Set<IAttributeValueCompound> compounds);
	
	public Set<IAttributeValueCompound> getAttributeValueCompounds();
	
}
