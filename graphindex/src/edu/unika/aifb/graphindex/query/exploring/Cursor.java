package edu.unika.aifb.graphindex.query.exploring;

import java.util.Set;

public class Cursor implements Comparable<Cursor> {

	private String m_keyword;
	private Cursor m_parent;
	private GraphElement m_element;
	private int m_distance;
	private int m_cost;
	
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
		m_cost = cost;
		if (m_parent != null)
			m_distance = m_parent.getDistance() + 1;
		else
			m_distance = 0;
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

	public GraphElement getGraphElement() {
		return m_element;
	}

	public int getCost() {
		return m_cost;
	}

	public int compareTo(Cursor o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Set<GraphElement> getParents() {
		// TODO Auto-generated method stub
		return null;
	}

}
