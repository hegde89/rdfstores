package edu.unika.aifb.keywordsearch;

import it.unimi.dsi.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;

import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.api.IResource;
import edu.unika.aifb.keywordsearch.impl.Attribute;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.impl.Relation;
import edu.unika.aifb.keywordsearch.impl.NamedConcept;


public class KeywordElement implements Comparable<KeywordElement>, Serializable {

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
//	private String query;
	private Set<String> keywords;
	private int docId;
	private Document doc;
	private BloomFilter bloomFilter;

	protected int type;
	
	public KeywordElement(IResource resource, int type, Document doc, double score) {
		this.resource = resource;
		this.type = type;
		this.matchingScore = score;
		this.doc = doc;
	}
	
	public KeywordElement(){}
	
	public KeywordElement(IResource resource, int type, int docID) {
		this.resource = resource;
		this.type = type;
		this.docId = docID;
	}

	public KeywordElement(IResource resource, int type, int docID, double score) {
		this.resource = resource;
		this.type = type;
		this.docId = docID;
		this.matchingScore = score;
	}
	
	public KeywordElement(IResource resource, int type, int docID, double score, String keyword) {
		this.resource = resource;
		this.type = type;
		this.docId = docID;
		this.matchingScore = score;
		this.keywords = new HashSet<String>();
		this.keywords.add(keyword);
	}
	
	public KeywordElement(IResource resource, int type, int docID, double score, Collection<String> keywords) {
		this.resource = resource;
		this.type = type;
		this.docId = docID;
		this.matchingScore = score;
		this.keywords = new HashSet<String>();
		this.keywords.addAll(keywords);
	}

	public int getType(){
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public void clearDoc() {
		this.doc = null;
	}
	
	public void setDocId(int docId) {
		this.docId = docId;
	}
	
	public int getDocId() {
		return docId;
	}
	
	public Collection<String> getKeywords() {
		return keywords;
	}
	
//	public String getKeyword() {
//		if(keywords == null || keywords.size() == 0) {
//			return null;
//		}
//		else
//			return keywords.iterator().next();
//			
//	}
	
	public void addKeyword(String keyword) {
		if(keywords != null) {
			keywords.add(keyword);
		}	
		else {
			keywords = new HashSet<String>();
			keywords.add(keyword);
		}
	}
	
	public void addKeywords(Collection<String> keywords) {
		if(keywords != null) {
			keywords.addAll(keywords);
		}	
		else {
			keywords = new HashSet<String>();
			keywords.addAll(keywords);
		}
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
	
	public void setBloomFilter(BloomFilter bf) {
		bloomFilter = bf;
	}
	
	public BloomFilter getBloomFilter() {
		if(bloomFilter == null) {
			byte[] bytes = doc.getFieldable(Constant.NEIGHBORHOOD_FIELD).binaryValue();
			ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(bytes);
			try {
				ObjectInputStream objectInput = new ObjectInputStream(byteArrayInput);
				bloomFilter = (BloomFilter)objectInput.readObject();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		return bloomFilter;
	}
	
	public Collection<KeywordElement> getReachable(Collection<KeywordElement> elements) {
		Collection<KeywordElement> result = new ArrayList<KeywordElement>();
		for(KeywordElement ele : elements) {
			if(isReachable(ele))
				result.add(ele);
		}
		return result;
	}
	
	public boolean isReachable(IEntity entity) {
		return getBloomFilter().contains(entity.getUri());
	}
	
	public boolean isReachable(KeywordElement ele) {
		if(ele.getType() != KeywordElement.ENTITY)
			return false;
		return getBloomFilter().contains(ele.getResource().getUri());
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
	
	public String getUri() {
		return resource.getUri(); 
	} 
	
	public String getExtensionId() {
		if(resource instanceof Entity) {
			return ((Entity)resource).getExtension(); 	
		}
		else 
			return null;
	} 
		
	public String toString(){
		if(resource == null)return null;
		if(resource instanceof NamedConcept)
			return ((NamedConcept)resource).getUri();
		else if(resource instanceof Attribute )
			return ((Attribute)resource).getUri();
		else if(resource instanceof Entity )
			return ((Entity)resource).getUri();
		else if(resource instanceof Relation )
			return ((Relation)resource).getUri();
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

	public int compareTo(KeywordElement o) {
		return this.getResource().getUri().compareTo(o.getResource().getUri());
	}

}
