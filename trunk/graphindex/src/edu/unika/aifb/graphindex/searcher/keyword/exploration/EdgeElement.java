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
import java.util.List;

import org.jgrapht.graph.DirectedMultigraph;

public class EdgeElement extends GraphElement {
	private NodeElement m_target;
	private NodeElement m_source;

	public EdgeElement(NodeElement source, String label, NodeElement target) {
		super(label);
		m_source = source;
		m_target = target;
	}

	public List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor) {
		List<GraphElement> neighbors = new ArrayList<GraphElement>();
		
		if (cursor.getParent() == null) {
			neighbors.add(m_target);
			neighbors.add(m_source);
		}
		else {
			NodeElement prev = (NodeElement)cursor.getParent().getGraphElement();
			if (m_source.equals(prev))
				neighbors.add(m_target);
			else
				neighbors.add(m_source);
		}
			
		return neighbors;
	}
	
	public NodeElement getSource() {
		return m_source;
	}
	
	public NodeElement getTarget() {
		return m_target;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_source == null) ? 0 : m_source.hashCode());
		result = prime * result + ((m_target == null) ? 0 : m_target.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EdgeElement other = (EdgeElement)obj;
		if (m_source == null) {
			if (other.m_source != null)
				return false;
		} else if (!m_source.equals(other.m_source))
			return false;
		if (m_target == null) {
			if (other.m_target != null)
				return false;
		} else if (!m_target.equals(other.m_target))
			return false;
		return true;
	}
	
	public String toString() {
		return m_label + "[" + m_keywordCursors.size() + "](" + m_source + "," + m_target + ")";
	}
}
