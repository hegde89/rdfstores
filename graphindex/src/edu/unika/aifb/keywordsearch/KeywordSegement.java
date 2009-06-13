package edu.unika.aifb.keywordsearch;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class KeywordSegement implements Comparable<KeywordSegement> {
	
	private SortedSet<String> keywords;
	private String query;
	
	public KeywordSegement() {
		this.keywords = new TreeSet<String>(); 
		this.query = "";
	}
	
	public KeywordSegement(String keyword) {
		this();
		this.addKeyword(keyword);
	}


	public KeywordSegement(Collection<String> keywords) {
		this();
		for(String element : keywords) {
			this.keywords.add(element);
			this.query += element + " ";
		}
			
	}
	
	public void addKeyword(String keyword) {
		this.keywords.add(keyword);
		this.query += keyword + " ";
	}
	
	public Set<String> getKeywords() {
		return this.keywords;
	} 
	
	public String getQuery() {
		return this.query;
	}
	
	public boolean contains(KeywordSegement segement) {
		if(keywords.containsAll(segement.keywords))
			return true;
		return false;
	}
	
	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof KeywordSegement)) return false;
		
		KeywordSegement ks = (KeywordSegement)object;
		if(this.keywords.equals(ks.keywords))
			return true;
		return false;
	}
	
	public int hashCode(){
		return keywords.hashCode();
	}
	
	public String toString() {
		String str = "";
		for(String keyword : keywords)
			str += keyword + " ";
		return str;
	}

	public int compareTo(KeywordSegement ks) {
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
