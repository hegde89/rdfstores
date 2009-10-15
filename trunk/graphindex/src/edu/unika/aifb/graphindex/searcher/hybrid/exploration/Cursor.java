package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public abstract class Cursor implements Comparable<Cursor> {

	protected Set<KeywordSegment> m_keywords;
	protected Cursor m_parent;
	protected GraphElement m_element;
	protected int m_distance;
	protected double m_cost;
	protected List<GraphElement> m_path = null;
	protected Set<GraphElement> m_parents;
	protected boolean m_finished;
	protected Set<String> m_inProperties = new HashSet<String>();
	protected Set<String> m_outProperties = new HashSet<String>();
	
	public Cursor(Set<KeywordSegment> keywords, GraphElement element) {
		this(keywords, element, null);
//		double score = 0;
//		for (KeywordSegment ks : keywords)
//			if (ks.getKeywords().size() > 1)
//				score += 0.5;
//		m_cost -= score;
		
		m_cost = element.getCost();
	}
	
	public Cursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		m_keywords = new HashSet<KeywordSegment>(keywords);
		m_element = element;
		m_parent = parent;
		if (m_parent != null) {
			m_distance = m_parent.getDistance() + 1;
			m_cost = m_parent.getCost();
		}
		else {
			m_distance = 0;
			m_cost = 0;
		}
		m_cost += m_element.getCost();
	}
	
	public abstract Cursor getNextCursor(GraphElement element);
	
	public int getDistance() {
		return m_distance;
	}
	
	public Set<KeywordSegment> getKeywordSegments() {
		return m_keywords;
	}
	
	public void addInProperties(Collection<String> properties) {
		m_inProperties.addAll(properties);
	}
	
	public Set<String> getInProperties() {
		return m_inProperties;
	}
	
	public void addOutProperties(Collection<String> properties) {
		m_outProperties.addAll(properties);
	}
	
	public Set<String> getOutProperties() {
		return m_outProperties;
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
	
	public void setCost(double cost) {
		m_cost = cost;
	}

	public double getCost() {
		return m_cost;
	}

	public int compareTo(Cursor o) {
		return ((Double)m_cost).compareTo(o.getCost());
	}
	
	public GraphElement getStartElement() {
		if (m_parent == null)
			return m_element;
		else
			return m_parent.getStartElement();
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
		result = prime * result + ((getPath() == null) ? 0 : getPath().hashCode());
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

	public void setFinished(boolean finished) {
		m_finished = finished;
	}
	
	public boolean isFinished() {
		return m_finished;
	}
}
