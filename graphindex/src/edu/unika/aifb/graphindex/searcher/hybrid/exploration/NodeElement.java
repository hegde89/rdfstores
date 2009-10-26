package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

/**
 * Copyright (C) 2009 G�nter Ladwig (gla at aifb.uni-karlsruhe.de)
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

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class NodeElement extends GraphElement {

	private Map<String,List<KeywordSegment>> m_augmentEdges;
	private Map<KeywordSegment,Table<String>> m_segmentEntities;
	private Set<String> m_inProperties;
	private Set<String> m_outProperties;
	
	public NodeElement(String label) {
		super(label);
		m_augmentEdges = new HashMap<String,List<KeywordSegment>>();
		m_segmentEntities = new HashMap<KeywordSegment,Table<String>>();
		m_inProperties = new HashSet<String>();
		m_outProperties = new HashSet<String>();
	}
	
	@Override
	public void reset() {
		super.reset();
		m_augmentEdges = new HashMap<String,List<KeywordSegment>>();
		m_segmentEntities = new HashMap<KeywordSegment,Table<String>>();
		m_inProperties = null;
		m_outProperties = null;
	}
	
	public void addInProperties(Set<String> props) {
		if (m_inProperties == null)
			m_inProperties = new HashSet<String>();
		m_inProperties.addAll(props);
	}

	public void addOutProperties(Set<String> props) {
		if (m_outProperties == null)
			m_outProperties = new HashSet<String>();
		m_outProperties.addAll(props);
	}
	
	public boolean acceptsEdge(EdgeElement edge) {
//		 {
//				if (m_keywordCursors.size() == 0)
//					return true;
//				
//				for (List<Cursor> cursors : m_keywordCursors.values())
//					for (Cursor c : cursors)
//						if (c.acceptsEdge(edge))
//							return true;
//				return false;
//			}
		
		if (edge.getTarget() == this && m_inProperties != null && !m_inProperties.contains(edge.getLabel()))
			return false;
		
		if (edge.getSource() == this && m_outProperties != null && !m_outProperties.contains(edge.getLabel()))
			return false;
		
		return true;
	}

	public double getCost() {
		return m_cost;
	}
	
	public void addFrom(NodeElement node) {
		for (String property : node.m_augmentEdges.keySet())
			for (KeywordSegment ks : node.m_augmentEdges.get(property))
				addAugmentedEdge(property, ks);
		for (KeywordSegment ks : node.m_segmentEntities.keySet())
			m_segmentEntities.put(ks, new Table<String>(node.m_segmentEntities.get(ks), true));
	}
	
	public void addAugmentedEdge(String property, KeywordSegment ks) {
		List<KeywordSegment> kss = m_augmentEdges.get(property);
		if (kss == null) {
			kss = new ArrayList<KeywordSegment>();
			m_augmentEdges.put(property, kss);
		}
		kss.add(ks);
	}
	
	public Table<String> getSegmentEntities(KeywordSegment ks) {
		return m_segmentEntities.get(ks);
	}
	
	public void addSegmentEntity(KeywordSegment segment, String entity) {
		Table<String> entities = m_segmentEntities.get(segment);
		if (entities == null) {
			entities = new Table<String>(m_label, segment.toString());
			m_segmentEntities.put(segment, entities);
		}
		entities.addRow(new String[] { entity, segment.toString() });
	}
	
	public Map<String,List<KeywordSegment>> getAugmentedEdges() {
		return m_augmentEdges;
	}
	
	public List<GraphElement> getNeighbors(Map<NodeElement,List<EdgeElement>> graph, Cursor cursor, Set<String> keywordEdges) {
		EdgeElement prevEdge = (EdgeElement)(cursor.getParent() != null ? cursor.getParent().getGraphElement() : null);
		Set<GraphElement> parents = cursor.getParents();

		List<GraphElement> neighbors = new ArrayList<GraphElement>();
		
		if (!cursor.isFinished()) {
			for (EdgeElement edge : graph.get(this)) {
				if (!keywordEdges.contains(edge.getLabel())) {
					if (edge.getSource() == this && !edge.getTarget().acceptsEdge(edge))
						continue;
					if (edge.getTarget() == this && !edge.getSource().acceptsEdge(edge))
						continue;
				}

				if (!edge.equals(prevEdge) && !parents.contains(edge.getSource()) && !parents.contains(edge.getTarget()))
					neighbors.add(edge);
			}
		}
		
		return neighbors;
	}
	
	public String toString() {
		return m_label + "[" + getCost() + "," + m_keywordCursors.size() + "]";
	}
}
