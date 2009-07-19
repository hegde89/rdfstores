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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class PrunedQueryPart extends StructuredQuery {
	private QNode m_root;
	
	private static final Logger log = Logger.getLogger(PrunedQueryPart.class);
	
	public PrunedQueryPart(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public QNode getRoot() {
		return m_root;
	}

	public List<QueryEdge> trim(int length) {
		if (m_queryGraph.edgeCount() < length)
			return new ArrayList<QueryEdge>();
		
		log.debug("trim " + this);

		QNode startNode = null;
		for (QNode node : m_queryGraph.vertexSet()) {
			if (node.isSelectVariable() || node.isConstant()) {
				startNode = node;
				break;
			}
				
		}
		log.debug("start node: " + startNode);
		m_root = startNode;
		
		HashMap<QNode,Integer> distances = new HashMap<QNode,Integer>();
		int max = calcDistances(startNode, 0, distances);
		int reclaimDistance = max - length;
		log.debug(distances + " " + max + " " + reclaimDistance);
		
		Set<QNode> reclaimedNodes = new HashSet<QNode>();
		List<QueryEdge> reclaimedEdges = new ArrayList<QueryEdge>();
		
		if (reclaimDistance > 0) {
			for (QNode node : distances.keySet()) {
				if (distances.get(node) <= reclaimDistance && !(node.isSelectVariable() || node.isConstant())) {
					Set<QNode> neighbors = new HashSet<QNode>();
					neighbors.addAll(m_queryGraph.predecessors(node));
					neighbors.addAll(m_queryGraph.successors(node));
					
					for (QNode neighbor : neighbors) {
						if (distances.get(neighbor) > distances.get(node)) {
							reclaimedNodes.add(node);
							break;
						}
					}
				}
			}
			
			log.debug(" reclaim node: " + reclaimedNodes);

			Set<QNode> notRemovedNodes = new HashSet<QNode>(reclaimedNodes);
			notRemovedNodes.add(startNode);
			
			for (QueryEdge edge : m_queryGraph.edgeSet()) {
				
				if (notRemovedNodes.contains(edge.getSource()) && notRemovedNodes.contains(edge.getTarget())) {
//					i.remove();
					reclaimedEdges.add(edge);
				}
			}
			
			m_queryGraph.removeAllEdges(reclaimedEdges);
			
			log.debug(" reclaimed edges: " + reclaimedEdges);
		}
		
		return reclaimedEdges;
	}
	
	private int calcDistances(QNode node, int distance, Map<QNode,Integer> distances) {
		distances.put(node, distance);
		
		int max = distance;
		
		for (QNode next : m_queryGraph.predecessors(node)) {
			if (distances.containsKey(next))
				continue;
			
			int dist = calcDistances(next, distance + 1, distances);
			if (dist > max)
				max = dist;
		}

		for (QNode next : m_queryGraph.successors(node)) {
			if (distances.containsKey(next))
				continue;
			
			int dist = calcDistances(next, distance + 1, distances);
			if (dist > max)
				max = dist;
		}

		return max;
	}

	public String toString() {
		return m_queryGraph.edgeSet().toString();
	}
}
