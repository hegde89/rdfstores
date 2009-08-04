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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

public class NodeElement extends GraphElement {

	private Set<String> m_entities;
	private Map<String,List<NodeElement>> m_augmentEdges;
	
	public NodeElement(String label) {
		super(label);
		m_entities = new HashSet<String>();
		m_augmentEdges = new HashMap<String,List<NodeElement>>();
	}
	
	public int getCost() {
		return 0;
	}
	
	public void addFrom(NodeElement node) {
		addEntities(node.getEntities());
		for (String property : node.m_augmentEdges.keySet())
			for (NodeElement target : node.m_augmentEdges.get(property))
				addAugmentedEdge(property, target);
	}
	
	public void addAugmentedEdge(String property, NodeElement target) {
		List<NodeElement> targets = m_augmentEdges.get(property);
		if (targets == null) {
			targets = new ArrayList<NodeElement>();
			m_augmentEdges.put(property, targets);
		}
		targets.add(target);
	}
	
	public Map<String,List<NodeElement>> getAugmentedEdges() {
		return m_augmentEdges;
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
		return m_label + "[" + m_keywordCursors.size() + "," + m_entities.size() + "," + m_augmentEdges.size() + "]";
	}

	public void addEntity(String uri) {
		m_entities.add(uri);
	}
	
	public void addEntities(Set<String> entities) {
		m_entities.addAll(entities);
	}

	public Set<String> getEntities() {
		return m_entities;
	}
}
