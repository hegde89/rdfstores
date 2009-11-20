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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class KeywordSegment implements Comparable<KeywordSegment> {
	
	private SortedSet<String> valueKeywords;
	private String attributeKeyword;
	private String query;
	
	public KeywordSegment() {
		this.valueKeywords = new TreeSet<String>(); 
		this.query = "";
	}
	
	public KeywordSegment(String keyword) {
		this();
		addKeyword(keyword);
	}


	public KeywordSegment(Collection<String> keywords) {
		this();
		addKeywords(keywords);
	}
	
	public void addKeyword(String keyword) {
		keyword = keyword.trim();
		boolean added = this.valueKeywords.add(keyword);
		if(added)
			this.query += keyword + " ";
	}
	
	public void addKeywords(Collection<String> keywords) {
		for(String keyword : keywords)
			addKeyword(keyword);
	}
	
	public Set<String> getKeywords() {
		return this.valueKeywords;
	} 
	
	public String getQuery() {
		return this.query;
	}
	
	public void addAttributeKeyword(String keyword) {
		this.attributeKeyword = keyword;
	} 
	
	public String getAttributeKeyword() {
		return this.attributeKeyword;
	} 
	
	public Set<String> getAllKeywords() {
		Set<String> allKeywords = new TreeSet<String>(); 
		allKeywords.addAll(valueKeywords);
		if(attributeKeyword != null)
			allKeywords.add(attributeKeyword);
		return allKeywords; 
	} 
	
	public boolean contains(KeywordSegment segement) {
		Iterator<String> iter = segement.valueKeywords.iterator();
		while (iter.hasNext()) {
			String keyword = iter.next();
		    if (!valueKeywords.contains(keyword) && 
		    		(attributeKeyword == null || !keyword.equals(attributeKeyword)))
		    	return false;
		}    
		return true;
	}
	
	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof KeywordSegment)) return false;
		
		KeywordSegment ks = (KeywordSegment)object;
		if((attributeKeyword != null && ks.attributeKeyword == null) || 
				(attributeKeyword == null && ks.attributeKeyword != null)) {
			return false;
		}
		else if(!attributeKeyword.equals(ks.attributeKeyword)) {
			return false;
		}
		if(this.valueKeywords.equals(ks.valueKeywords)) {
			return true;
		}
			
		return false;
	}
	
	public int hashCode(){
		int h = valueKeywords.hashCode();
		h = attributeKeyword == null ? h : h + 37*attributeKeyword.hashCode();  
		return h;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		sb.append(attributeKeyword == null ? "" : attributeKeyword + " | ");
		Iterator<String> iter = valueKeywords.iterator();
		for (;;) {
		    String str = iter.next();
		    sb.append(str);
		    if (!iter.hasNext())
			return sb.append(']').toString();
		    sb.append(", ");
		}
	}

	public int compareTo(KeywordSegment ks) {
		SortedSet<String> s1 = this.valueKeywords;
		SortedSet<String> s2 = ks.valueKeywords;
		int size1 = s1.size() + (attributeKeyword == null ? 0 : 1);
		int size2 = s2.size() + (ks.attributeKeyword == null ? 0 : 1);
		if (size1 < size2) {
			return 1;
		} else if (size1 > size2) {
			return -1;
		} else if(attributeKeyword == null && ks.attributeKeyword != null) {
			return 1;
		} else if(attributeKeyword != null && ks.attributeKeyword == null) {
			return -1;
		} else if(attributeKeyword != null && ks.attributeKeyword != null) {
			return attributeKeyword.compareTo(ks.attributeKeyword);
		} 
		else {
			Iterator<String> i1 = s1.iterator();
			Iterator<String> i2 = s2.iterator();
			int c = 0;
			while (i1.hasNext() && i2.hasNext() && c == 0) {
				c = i1.next().compareTo(i2.next());
			}
			return c;
		}
	}

}
