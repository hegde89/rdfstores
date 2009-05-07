package edu.unika.aifb.keywordsearch;

import java.io.Serializable;
import java.util.Set;

import edu.unika.aifb.keywordsearch.api.IResource;
import edu.unika.aifb.keywordsearch.impl.Attribute;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.impl.Relation;
import edu.unika.aifb.keywordsearch.impl.NamedConcept;


public class KeywordElement implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8594203186544962955L;
	
	public static final int CONCEPT = 0;
	public static final int RELATION = 1;
	public static final int ENTITY = 2;
	public static final int ATTRIBUTE = 3;

	private IResource resource;
	private double matchingScore;
	private String query;

	protected int type;
	
	public KeywordElement(){}

	public KeywordElement(IResource resource, int type) {
		this.resource = resource;
		this.type = type;
	}

	public KeywordElement(IResource resource, int type, double score) {
		this.resource = resource;
		this.type = type;
		this.matchingScore = score;
	}
	
	public KeywordElement(IResource resource, int type, double score, String query) {
		this.resource = resource;
		this.type = type;
		this.matchingScore = score;
		this.query = query;
	}

	public int getType(){
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public String getQuery(){
		return query;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}

	public void setResource(IResource resource){
		this.resource = resource;
	}

	public IResource getResource(){
		return resource;
	}

	public void setMatchingScore(double score){
		this.matchingScore = score;
	}

	public double getMatchingScore(){
		return matchingScore;
	}
		
	public String toString(){
		if(resource == null)return null;
		if(resource instanceof NamedConcept)
			return ((NamedConcept)resource).getUri();
		else if(resource instanceof Attribute )
			return ((Attribute)resource).getUri();
		else if(resource instanceof Entity )
			return ((Entity)resource).getUri();
		else return super.toString();
	}

	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof KeywordElement)) return false;
		
		KeywordElement vertex = (KeywordElement)object;
		if(resource instanceof NamedConcept && vertex.getResource() instanceof NamedConcept)
			return ((NamedConcept)resource).getUri().equals(((NamedConcept)vertex.getResource()).getUri());
		else if(resource instanceof Relation && vertex.getResource() instanceof Relation)
			return ((Relation)resource).getUri().equals(((Relation)vertex.getResource()).getUri());
		else if(resource instanceof Entity && vertex.getResource() instanceof Entity) {
			return ((Entity)resource).getUri().equals(((Entity)vertex.getResource()).getUri());
		}
		return false;
	}
	
	public int hashCode(){
		return resource.hashCode();
	}

}
