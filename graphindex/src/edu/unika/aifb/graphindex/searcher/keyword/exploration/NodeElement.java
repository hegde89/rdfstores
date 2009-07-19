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

public class NodeElement extends GraphElement {

	private String m_attributeUri;
	
	public NodeElement(String label) {
		super(label);
	}
	
	public NodeElement(String label, String attributeUri) {
		super(label);
		m_attributeUri = attributeUri;
	}
	
	public String getAttributeUri() {
		return m_attributeUri;
	}
	
	public void setAttributeUri(String uri) {
		m_attributeUri = uri;
	}

	public List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor) {
		EdgeElement prevEdge = (EdgeElement)(cursor.getParent() != null ? cursor.getParent().getGraphElement() : null);

		List<GraphElement> neighbors = new ArrayList<GraphElement>();
		
		for (EdgeElement edge : graph.edgesOf(this)) {
			if (!edge.equals(prevEdge))
				neighbors.add(edge);
		}
		
		return neighbors;
	}
	
	public String toString() {
		return m_label + "[" + m_keywordCursors.size() + "]";
	}
}
