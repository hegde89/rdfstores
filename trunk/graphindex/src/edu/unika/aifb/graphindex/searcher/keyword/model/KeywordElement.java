package edu.unika.aifb.graphindex.searcher.keyword.model;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;

import edu.unika.aifb.graphindex.model.IResource;
import edu.unika.aifb.graphindex.model.impl.Attribute;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.model.impl.NamedConcept;
import edu.unika.aifb.graphindex.model.impl.Relation;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.keyword.BloomFilter;


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
	private Set<String> keywords;
	private Set<String> reachableKeywords; 
	private Document doc;
	private BloomFilter bloomFilter;

	protected int type;

	private NeighborhoodStorage ns;
	
	public KeywordElement(IResource resource, int type, Document doc, double score) {
		this.resource = resource;
		this.type = type;
		this.matchingScore = score;
		this.doc = doc;
	}
	
	public KeywordElement(IResource resource, int type, double score, String keyword) {
		this.resource = resource;
		this.type = type;
		this.matchingScore = score;
		this.keywords = new HashSet<String>();
		this.keywords.add(keyword);
	}
	
	public KeywordElement(IResource resource, int type, NeighborhoodStorage ns) {
		this.resource = resource;
		this.type = type;
		this.ns = ns;
	}
	
	public KeywordElement(){}
	
	public int getType(){
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public void clearDoc() {
		this.doc = null;
	}
	
	public void setKeywords(Set<String> keywords) {
		this.keywords = keywords;
	}
	
	public void addKeywords(Set<String> keywords) {
		if(this.keywords == null) {
			this.keywords = new HashSet<String>();
		}
		for(String keyword : keywords) {
			this.keywords.add(keyword);
		}
	}
	
	public Collection<String> getKeywords() {
		return keywords;
	}
	
	public void addReachableKeywords(Collection<String> keywords) {
		if(reachableKeywords == null) {
			reachableKeywords = new HashSet<String>();
		}
		reachableKeywords.addAll(keywords);
			
	}
	
	public void addReachableKeyword(String keyword) {
		if(reachableKeywords == null) {
			reachableKeywords = new HashSet<String>();
		}
		reachableKeywords.add(keyword);
	}
	
	public Collection<String> getReachableKeywords() {
		return reachableKeywords;
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
//			try {
//				bloomFilter = ns.getNeighborhoodBloomFilter(resource.getUri());
//			} catch (StorageException e) {
//				e.printStackTrace();
//			}
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
	
	public boolean isReachable(KeywordElement ele) {
		if(ele.getType() != KeywordElement.ENTITY)
			return false;
		if(this.equals(ele))
			return true;
		String uri = ele.getResource().getUri();
		if(uri.startsWith("http://www."))
			uri = uri.substring(11);
		else if(uri.startsWith("http://"))
			uri = uri.substring(7);
		return getBloomFilter().contains(uri);
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
		else if (resource instanceof NamedConcept)
			return ((NamedConcept)resource).getExtension();
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
		return getResource().getUri().equals(vertex.getResource().getUri());
	}
	
	public int hashCode(){
		return resource.hashCode();
	}

	public int compareTo(KeywordElement o) {
		return this.getResource().getUri().compareTo(o.getResource().getUri());
	}

}
