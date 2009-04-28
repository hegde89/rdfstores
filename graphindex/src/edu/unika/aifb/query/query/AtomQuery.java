package edu.unika.aifb.query.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class AtomQuery extends AbstractQuery {
	private String subject;
	private String predicate;
	private String object;
	
	public AtomQuery(String subject, String predicate, String object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		path = new ArrayList<AtomQuery>();
//		path = new HashSet<AtomQuery>();
	}
	
	public String getQuery() {
		return subject + " " + predicate + " " + object + "\n";
	}
	
	public String getSubject() {
		return subject;
	} 
	
	public void setSubject(String sub) {
		subject = sub;
	} 
	
	public String getPredicate() {
		return predicate;
	} 
	
	public String getObject() {
		return object;
	} 
	
	public void setObject(String obj) {
		object = obj;
	} 
	
	public String toString() {
		return getQuery();
	} 
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if(!(o instanceof AtomQuery)) return false;
		AtomQuery aq = (AtomQuery)o;
		if(!subject.equals(aq.getSubject())) return false;
		if(!predicate.equals(aq.getPredicate())) return false;
		if(!object.equals(aq.getObject())) return false;
		return true;
	}
	
	public int hashCode() {
		return 7*subject.hashCode() 
			+ 11*predicate.hashCode()
			+ 13*object.hashCode();
	}

	public int getNumAtom() {
		return 1;
	}

	public int getNumVar() {
		return 1;
	}

	public int getNumDvar() {
		return 1;
	}
}
