package edu.unika.aifb.query.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EntityQuery extends AbstractQuery {
	
	public EntityQuery() {
		path = new ArrayList<AtomQuery>();
//		path = new HashSet<AtomQuery>();
	} 
	
	public void addAtom(String subject, String predicate, String object) {
		path.add(new AtomQuery(subject, predicate, object));
	}

	public String getQuery() {
		String query = "";
		for(AtomQuery aq : path) {
			query += aq;
		}
		return query; 
	}
	
	public List<AtomQuery> getPath() {
		return path;
	}
	
//	public Set<AtomQuery> getPath() {
//		return path;
//	}
	
	public String toString() {
		return getQuery();
	} 
	
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PathQuery)) return false;
		PathQuery other = (PathQuery) o;
		if (!(path.equals(other.getPath()))) return false;
		return true;
	}

	public int hashCode() {
		return 13 * path.hashCode();
	}

	public int getNumAtom() {
		return numAtom;
	}

	public int getNumVar() {
		return numVar;
	}

	public int getNumDvar() {
		return numDVar;
	}

}
