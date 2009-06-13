package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Cursor implements Comparable<Cursor> {

	private String m_keyword;
	private Cursor m_parent;
	private GraphElement m_element, m_startElement;
	private int m_distance;
	private int m_cost;
	private List<GraphElement> m_path = null;
	private Set<GraphElement> m_parents;
	private boolean m_fakeStart = false;
	
	public Cursor(String keyword, GraphElement element) {
		m_keyword = keyword;
		m_element = element;
		m_parent = null;
		m_distance = 0;
		m_cost = 0;
	}
	
	public Cursor(String keyword, GraphElement element, Cursor parent, int cost) {
		this(keyword, element);
		m_parent = parent;
		if (m_parent != null) {
			m_distance = m_parent.getDistance() + 1;
		}	
		else
			m_distance = 0;
		m_cost = m_distance;
	}
	
	public void setFakeStart(boolean fs) {
		m_fakeStart = fs;
	}
	
	public boolean isFakeStart() {
		return m_fakeStart;
	}
	
	public int getDistance() {
		return m_distance;
	}
	
	public String getKeyword() {
		return m_keyword;
	}

	public Cursor getParent() {
		return m_parent;
	}
	
	public Cursor getStartCursor() {
		if (m_parent == null)
			return this;
		else
			return m_parent.getStartCursor();
	}
	
	public GraphElement getGraphElement() {
		return m_element;
	}

	public int getCost() {
		return m_cost;
	}

	public int compareTo(Cursor o) {
		return ((Integer)m_cost).compareTo(o.getCost());
	}
	
	public GraphElement getStartElement() {
		if (m_parent == null)
			m_startElement = m_element;
		else
			m_startElement =  m_parent.getStartElement();
		return m_startElement;
	}
	
	public List<GraphElement> getPath() {
		if (m_path == null) {
			if (m_parent == null) 
				m_path = new ArrayList<GraphElement>();
			else
				m_path = new ArrayList<GraphElement>(m_parent.getPath());
			m_path.add(m_element);
		}
		
		return m_path;
	}

	public Set<GraphElement> getParents() {
		if (m_parents == null) {
			m_parents = new HashSet<GraphElement>();
			List<GraphElement> path = getPath();
			m_parents.addAll(path);
			m_parents.remove(m_element);
		}
		
		return m_parents;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_element == null) ? 0 : m_element.hashCode());
		result = prime * result + ((m_keyword == null) ? 0 : m_keyword.hashCode());
		result = prime * result + ((m_path == null) ? 0 : m_path.hashCode());
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
		Cursor other = (Cursor)obj;
		if (m_element == null) {
			if (other.m_element != null)
				return false;
		} else if (!m_element.equals(other.m_element))
			return false;
		if (m_keyword == null) {
			if (other.m_keyword != null)
				return false;
		} else if (!m_keyword.equals(other.m_keyword))
			return false;
		if (!getPath().equals(other.getPath()))
			return false;
		return true;
	}

	public String toString() {
		return "(" + m_keyword + "," + m_element + "," + m_distance + "," + m_cost + ")";
	}
}
