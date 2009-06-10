package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.query.model.Query;

public class Subgraph extends DirectedMultigraph<NodeElement,EdgeElement> implements Comparable<Subgraph> {
	private static final long serialVersionUID = -5730502189634789126L;
	
	private List<Cursor> m_cursors;
	private Set<EdgeElement> m_edges;
	private int m_cost;
	
	private static final Logger log = Logger.getLogger(Subgraph.class);
	
	public Subgraph(Class<? extends EdgeElement> arg0) {
		super(arg0);
		
		m_edges = new HashSet<EdgeElement>();
	}

	public Subgraph(List<Cursor> cursors) {
		this(EdgeElement.class);
		
		m_cursors = cursors;
		for (Cursor c : cursors) {
			m_cost += c.getCost();
			for (GraphElement e : c.getPath()) {
				if (e instanceof EdgeElement) {
					m_edges.add((EdgeElement)e);
					addVertex(((EdgeElement)e).getSource());
					addVertex(((EdgeElement)e).getTarget());
					addEdge(((EdgeElement)e).getSource(), ((EdgeElement)e).getTarget(), (EdgeElement)e);
				}
			}
		}
	}
	
	public int getCost() {
		return m_cost;
	}

	public int compareTo(Subgraph o) {
		return ((Integer)getCost()).compareTo(o.getCost());
	}
	
	public Query toQuery() {
		List<GraphElement> startElements = new ArrayList<GraphElement>();
		for (Cursor c : m_cursors) {
			startElements.add(c.getStartElement());
		}
		log.debug(startElements);
		
		return null;
	}
	
	public String toString() {
		return "subgraph size: " + m_edges.size() + ", cost: " + m_cost + ", " + m_edges;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_edges == null) ? 0 : m_edges.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Subgraph other = (Subgraph)obj;
		if (m_edges == null) {
			if (other.m_edges != null)
				return false;
		} else if (!m_edges.equals(other.m_edges))
			return false;
		return true;
	}
}
