package edu.unika.aifb.graphindex.searcher.keyword.exploration;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class Cursor implements Comparable<Cursor> {

	private Set<KeywordSegment> m_keywords;
	private Cursor m_parent;
	protected GraphElement m_element;
	private GraphElement m_startElement;
	private int m_distance;
	private int m_cost;
	private List<GraphElement> m_path = null;
	private Set<GraphElement> m_parents;
	private boolean m_fakeStart = false;
	
	public Cursor(KeywordSegment keyword, GraphElement element) {
		this(keyword, element, null, 0);
	}
	
	public Cursor(KeywordSegment keyword, GraphElement element, Cursor parent, int cost) {
		this(new HashSet<KeywordSegment>(Arrays.asList(keyword)), element, parent, cost);
	}
	
	public Cursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent, int cost) {
		m_keywords = new HashSet<KeywordSegment>(keywords);
		m_element = element;
		m_parent = parent;
		if (m_parent != null) {
			m_distance = m_parent.getDistance() + 1;
		}	
		else
			m_distance = 0;
		m_cost = m_distance + 1;
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
	
	public Set<KeywordSegment> getKeywordSegments() {
		return m_keywords;
	}
	
	public void addKeywordSegment(KeywordSegment ks) {
		m_keywords.add(ks);
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
	
	public Set<EdgeElement> getEdges() {
		Set<EdgeElement> edges = new HashSet<EdgeElement>();
		for (GraphElement e : getPath())
			if (e instanceof EdgeElement)
				edges.add((EdgeElement) e);
		return edges;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_element == null) ? 0 : m_element.hashCode());
		result = prime * result + ((m_keywords == null) ? 0 : m_keywords.hashCode());
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
		if (m_keywords == null) {
			if (other.m_keywords != null)
				return false;
		} else if (!m_keywords.equals(other.m_keywords))
			return false;
		if (!getPath().equals(other.getPath()))
			return false;
		return true;
	}

	public String toString() {
		return "(" + m_keywords + "," + m_element + "," + m_distance + "," + m_cost + ")";
	}
}
