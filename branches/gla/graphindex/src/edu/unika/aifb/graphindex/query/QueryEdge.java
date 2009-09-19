package edu.unika.aifb.graphindex.query;

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

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;

public class QueryEdge extends DefaultEdge {
	private static final long serialVersionUID = -2202586718853035604L;
	
	private QueryGraph m_graph;
	private QNode m_src, m_trg;
	private String m_property;
	
	public static final int IS_SRC = 0;
	public static final int IS_SRCDST = 1;
	public static final int IS_DSTSRC = 2;
	public static final int IS_NONE = 3;
	public static final int IS_DST = 4;

	public QueryEdge(QNode src, QNode trg, String property, QueryGraph graph) {
		m_src = src;
		m_trg = trg;
		m_property = property;
		m_graph = graph;
	}

	public QueryEdge(QNode src, QNode trg, String property) {
		this(src, trg, property, null);
	}
	
	public void setGraph(QueryGraph graph) {
		m_graph = graph;
	}

	public QueryGraph getGraph() {
		return m_graph;
	}
	
	public QNode getSource() {
		return m_src;
	}
	
	public QNode getTarget() {
		return m_trg;
	}
	
	public String getProperty() {
		return m_property;
	}
	
	public String getLabel() {
		return m_property;
	}
	
	public String toString() {
		return m_property + "(" + m_src + "," + m_trg + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_property == null) ? 0 : m_property.hashCode());
		result = prime * result + ((m_src == null) ? 0 : m_src.hashCode());
		result = prime * result + ((m_trg == null) ? 0 : m_trg.hashCode());
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
		QueryEdge other = (QueryEdge)obj;
		if (m_property == null) {
			if (other.m_property != null)
				return false;
		} else if (!m_property.equals(other.m_property))
			return false;
		if (m_src == null) {
			if (other.m_src != null)
				return false;
		} else if (!m_src.equals(other.m_src))
			return false;
		if (m_trg == null) {
			if (other.m_trg != null)
				return false;
		} else if (!m_trg.equals(other.m_trg))
			return false;
		return true;
	}

	public int intersect(QueryEdge e) {
		if (getSource() == e.getSource())
			return IS_SRC;
		if (getTarget() == e.getTarget())
			return IS_DST;
		if (getSource() == e.getTarget())
			return IS_SRCDST;
		if (getTarget() == e.getSource())
			return IS_DSTSRC;
		return IS_NONE;
	}
}
