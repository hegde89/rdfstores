package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Triple;

public class TripleList implements Iterable<Triple>{
	private List<Triple> m_triples;
	private static final Logger log = Logger.getLogger(TripleSet.class);
	
	public TripleList() {
		this(new ArrayList<Triple>());
	}
	
	public TripleList(List<Triple> triples) {
		m_triples = triples;
	}
	
	public List<Triple> getTriples() {
		return m_triples;
	}
	
	public void addTriples(List<Triple> triples) {
		m_triples.addAll(triples);
	}
	
	public Set<String> getSubjects() {
		Set<String> subjects = new HashSet<String>();
		for (Triple t : m_triples)
			subjects.add(t.getSubject());
		return subjects;
	}
	
	public Set<String> getObjects() {
		Set<String> objects = new HashSet<String>();
		for (Triple t : m_triples)
			objects.add(t.getObject());
		return objects;
	}
	
	public void refineWith(Set<String> objects) {
		List<Triple> triples = new LinkedList<Triple>();
		for (Iterator<Triple> i = iterator(); i.hasNext(); ) {
			Triple t = i.next();
			if (objects.contains(t.getObject()))
				triples.add(t);
		}
		m_triples = triples;
	}
	
	public Iterator<Triple> iterator() {
		return m_triples.iterator();
	}

	public void addTriple(Triple triple) {
		m_triples.add(triple);
	}

	public int size() {
		return m_triples.size();
	}
	
	public String toString() {
		return m_triples.toString();
	}
}
