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
	
	private SortedSet<String> keywords;
	private String query;
	
	public KeywordSegment() {
		this.keywords = new TreeSet<String>(); 
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
		boolean added = this.keywords.add(keyword);
		if(added)
			this.query += keyword + " ";
	}
	
	public void addKeywords(Collection<String> keywords) {
		for(String keyword : keywords)
			addKeyword(keyword);
	}
	
	public Set<String> getKeywords() {
		return this.keywords;
	} 
	
	public String getQuery() {
		return this.query;
	}
	
	public boolean contains(KeywordSegment segement) {
		if(keywords.containsAll(segement.keywords))
			return true;
		return false;
	}
	
	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof KeywordSegment)) return false;
		
		KeywordSegment ks = (KeywordSegment)object;
		if(this.keywords.equals(ks.keywords))
			return true;
		return false;
	}
	
	public int hashCode(){
		return keywords.hashCode();
	}
	
	public String toString() {
		return keywords.toString();
	}

	public int compareTo(KeywordSegment ks) {
		SortedSet<String> s1 = this.keywords;
		SortedSet<String> s2 = ks.keywords;
		if (s1.size() < s2.size()) {
			return 1;
		} else if (s1.size() > s2.size()) {
			return -1;
		} else {
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
