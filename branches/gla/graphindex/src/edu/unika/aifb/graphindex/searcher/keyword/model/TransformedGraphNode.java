package edu.unika.aifb.graphindex.searcher.keyword.model;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.query.QNode;

public class TransformedGraphNode {
	
	public static final int ENTITY_QUERY_NODE = 0;
	public static final int ENTITY_NODE = 1;
	
	private int m_type;
	
	private QNode m_nodeId;
	private String m_nodeName;
	private Map<QNode,Integer> m_distances;
	private Map<String, Collection<String>> m_attributeQueries;
	private Collection<TransformedGraphNode> m_neighbors;
	private Collection<KeywordElement> m_entities;
	private int m_maxDistance;
	private Set<String> m_typeQueries; 
	
	private boolean m_isVisisted;
	private TransformedGraphNode m_filter;
	private int m_pathLength;
	
	
	public TransformedGraphNode(QNode node, String name, int type) {
		this.m_nodeId = node;
		this.m_nodeName = name;
		this.m_type = type;
		this.m_distances = new HashMap<QNode, Integer>();
		this.m_attributeQueries = new HashMap<String, Collection<String>>();
		this.m_typeQueries = new HashSet<String>();
		this.m_neighbors = new HashSet<TransformedGraphNode>();
		this.m_maxDistance = 0;
		this.m_isVisisted = false;
		this.m_pathLength = -1;
	}
	
	public void setPathLength(int length) {
		m_pathLength = length;
	}
	
	public int getPathLength() {
		return m_pathLength;
	}
	
	public void setFilter(TransformedGraphNode node) {
		m_filter = node;
	}
	
	public TransformedGraphNode getFilter() {
		return m_filter;
	} 
	
	public void setVisisted() {
		m_isVisisted = true;
	}
	
	public boolean isVisited() {
		return m_isVisisted;
	}
	
	public QNode getNodeId() {
		return this.m_nodeId;
	}
	
	public String getNodeName() {
		return m_nodeName;
	}
	
	public int getType() {
		return m_type; 
	}
	
	public void setEntities(Collection<KeywordElement> entities) {
		this.m_entities = entities;
	}
	
	public Collection<KeywordElement> getEntities() {
		return this.m_entities;
	} 
	
	public void removeEntity(KeywordElement element) {
		if(m_entities != null)
			m_entities.remove(element);
	}
	
	public void addTypeQuery(String type) {
		m_typeQueries.add(type);
	}
	
	public Collection<String> getTypeQueries() {
		return m_typeQueries;
	}
	
	public void addAttributeQuery(String predicate, String object) {
		Collection<String> coll = this.m_attributeQueries.get(predicate);
		if(coll == null) {
			coll = new HashSet<String>();
			m_attributeQueries.put(predicate, coll);
		}
		coll.add(object);
	}
	
	public Map<String, Collection<String>> getAttributeQueries() {
		return this.m_attributeQueries;
	}
	
	public String getUriQuery() {
		if(m_type == ENTITY_NODE)
			return m_nodeName;
		else 
			return null;
	}
	
	public void setDistance(QNode nodeId, int dis) {
		if(!m_distances.containsKey(nodeId)) {
			m_distances.put(nodeId, dis);
			if(dis > m_maxDistance)
				m_maxDistance = dis;
		}	
	}
	
	public int getDistance(QNode nodeId) {
		return m_distances.get(nodeId); 
	}
	
	public int getMaxDistance() {
		return m_maxDistance;
	}
	
	public void addNeighbor(TransformedGraphNode node) {
		this.m_neighbors.add(node);
	}
	
	public Collection<TransformedGraphNode> getNeighbors() {
		return this.m_neighbors;
	}
	
	public int getNumOfEntities() {
		if(m_entities == null)
			return 0;
		return m_entities.size();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_nodeId.hashCode();
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
		TransformedGraphNode other = (TransformedGraphNode)obj;
		if (m_nodeId.equals(other.getNodeId()))
			return false;
		return true;
	}

	public String toString() {
		return getNodeName();
	}
}
