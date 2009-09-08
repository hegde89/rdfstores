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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.alg.ConnectivityInspector;

public class PrunedQueryPart extends StructuredQuery {
	private PrunedQuery m_prunedQuery;
	private QNode m_root;
	private List<QueryEdge> m_reclaimedEdges;
	
	private static final Logger log = Logger.getLogger(PrunedQueryPart.class);
	
	public PrunedQueryPart(String name, PrunedQuery prunedQuery) {
		super(name);
		m_prunedQuery = prunedQuery;
		m_reclaimedEdges = new ArrayList<QueryEdge>();
	}
	
	public QNode getRoot() {
		return m_root;
	}
	
	public List<QueryEdge> getReclaimedEdges() {
		return m_reclaimedEdges;
	}
	
	public List<PrunedQueryPart> trim(int length) {
//		if (m_queryGraph.edgeCount() < length) {
//			return new ArrayList<PrunedQueryPart>(Arrays.asList(this));
//		}
		
		log.debug("trim " + this);

		QNode startNode = null;
		for (QNode node : m_queryGraph.vertexSet()) {
			if (node.isSelectVariable() || node.isConstant()) {
				startNode = node;
				break;
			}
		}
		
		if (startNode == null) {
			int maxDegree = 0;
			for (QNode node : m_queryGraph.vertexSet()) {
				if (m_queryGraph.inDegreeOf(node) + m_queryGraph.outDegreeOf(node) > maxDegree) {
					startNode = node;
					maxDegree = m_queryGraph.inDegreeOf(node) + m_queryGraph.outDegreeOf(node);
				}
			}
		}
		
		log.debug("start node: " + startNode);
		m_root = startNode;
		
		HashMap<QNode,Integer> distances = new HashMap<QNode,Integer>();
		int max = calcDistances(startNode, 0, distances);
		int reclaimDistance = max - length;
		log.debug(distances + " " + max + " " + reclaimDistance);
		
		Set<QNode> reclaimedNodes = new HashSet<QNode>();
		m_reclaimedEdges = new ArrayList<QueryEdge>();

		List<PrunedQueryPart> parts = new ArrayList<PrunedQueryPart>();

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
					m_reclaimedEdges.add(edge);
				}
			}
			
			m_queryGraph.removeAllEdges(m_reclaimedEdges);
			
			ConnectivityInspector<QNode,QueryEdge> connectivityInspector = new ConnectivityInspector<QNode,QueryEdge>(m_queryGraph);
			List<Set<QNode>> components = connectivityInspector.connectedSets();

			for (Set<QNode> component : components) {
				if (component.size() <= 1)
					continue;
				
				PrunedQueryPart part = new PrunedQueryPart("part", m_prunedQuery);
				QNode root = null;

				for (QNode node : component) {
					for (QueryEdge edge : m_queryGraph.outgoingEdgesOf(node))
						part.addEdge(edge.getSource(), edge.getLabel(), edge.getTarget());
					
					if (reclaimedNodes.contains(node) || node.isSelectVariable())
						root = node;
				}
				
				part.m_root = root;
				parts.add(part);
				log.debug("new part: " + part + " " + part.getRoot());
			}
		}
		else
			parts.add(this);
		
		return parts;
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
