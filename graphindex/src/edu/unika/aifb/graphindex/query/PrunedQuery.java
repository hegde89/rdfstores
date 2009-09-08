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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.util.Util;

public class PrunedQuery extends StructuredQuery {

	private StructureIndex m_si;
	private QueryGraph m_prunedQueryGraph;
	private Set<QNode> m_removeNodes;
	private Set<String> m_neutralEdgeSet;
	private Set<PrunedQueryPart> m_prunedParts;
	private Set<QNode> m_roots;
	
	private static final Logger log = Logger.getLogger(PrunedQuery.class);

	public PrunedQuery(StructuredQuery query, StructureIndex si) {
		super(query.getName() + "-pruned");
		
		m_si = si;
		m_selectVariables = query.getSelectVariables();
		
		pruneQuery(query, si);
	}
	
	public boolean isRootOfPrunedPart(String node) {
		QNode n = m_queryGraph.getNodeByLabel(node);
		return m_roots.contains(n);
	}
	
	public Set<PrunedQueryPart> getPrunedParts() {
		return m_prunedParts;
	}
	
	public QueryGraph getPrunedQueryGraph() {
		return m_prunedQueryGraph;
	}
	
	public void pruneQuery(StructuredQuery query, StructureIndex index) {
		Set<String> indexEdges = new HashSet<String>();
		indexEdges.addAll(index.getBackwardEdges());
		indexEdges.addAll(index.getForwardEdges());
		
		m_queryGraph = query.getQueryGraph().deepCopy();
		m_prunedQueryGraph = pruneQueryGraph(index);
	}
	
	@SuppressWarnings("unchecked")
	private QueryGraph pruneQueryGraph(StructureIndex index) {
		m_removeNodes = new HashSet<QNode>();
		m_neutralEdgeSet = new HashSet<String>();
		m_roots = new HashSet<QNode>();
		
		Set<QNode> fixedNodes = new HashSet<QNode>();
		for (QNode node : m_queryGraph.vertexSet())
			if (node.isConstant() || node.isSelectVariable())
				fixedNodes.add(node);
		
		for (QNode start : new HashSet<QNode>(fixedNodes))
			explorePath(m_queryGraph, start, new ArrayList<QNode>(), fixedNodes);
		
//		log.debug(fixedNodes);
//		log.debug(m_queryGraph.edgeSet());
		
		QueryGraph prunedQueryGraph = new QueryGraph();
		Map<QNode,PrunedQueryPart> parts = new HashMap<QNode,PrunedQueryPart>();
		for (QueryEdge e : m_queryGraph.edgeSet()) {
			if (fixedNodes.contains(e.getSource()) && fixedNodes.contains(e.getTarget()))
				prunedQueryGraph.addEdge(e.getSource(), e.getLabel(), e.getTarget());
			else {
				if (parts.containsKey(e.getSource()) && parts.containsKey(e.getTarget())) {
					PrunedQueryPart srcPart = parts.get(e.getSource());
					PrunedQueryPart trgPart = parts.get(e.getTarget());
					
					srcPart.addEdge(e.getSource(), e.getLabel(), e.getTarget());
					if (srcPart != trgPart) {
						for (QueryEdge trgEdge : trgPart.getQueryGraph().edgeSet()) {
							parts.put(trgEdge.getSource(), srcPart);
							parts.put(trgEdge.getTarget(), srcPart);
						}
					}
				}
				else if (parts.containsKey(e.getSource()) && !parts.containsKey(e.getTarget())) {
					PrunedQueryPart part = parts.get(e.getSource());
					part.addEdge(e.getSource(), e.getLabel(), e.getTarget());
					parts.put(e.getTarget(), part);
				}
				else if (!parts.containsKey(e.getSource()) && parts.containsKey(e.getTarget())) {
					PrunedQueryPart part = parts.get(e.getTarget());
					part.addEdge(e.getSource(), e.getLabel(), e.getTarget());
					parts.put(e.getSource(), part);
				}
				else {
					PrunedQueryPart part = new PrunedQueryPart(getName() + "-part", this);
					part.addEdge(e.getSource(), e.getLabel(), e.getTarget());
					parts.put(e.getSource(), part);
					parts.put(e.getTarget(), part);
				}
			}
		}
		
		m_prunedParts = new HashSet<PrunedQueryPart>();

		for (PrunedQueryPart oldPart : new HashSet<PrunedQueryPart>(parts.values())) {
			List<PrunedQueryPart> newPrunedParts = oldPart.trim(m_si.getPathLength());
			for (PrunedQueryPart part : newPrunedParts) {
				log.debug(part.getRoot() + " " + part);
				m_prunedParts.add(part);
				m_roots.add(part.getRoot());
				m_removeNodes.addAll(part.getQueryGraph().vertexSet());
				for (QueryEdge edge : part.getReclaimedEdges())
					prunedQueryGraph.addEdge(edge.getSource(), edge.getLabel(), edge.getTarget());
			}
		}

		log.debug("pruning: removed " + m_removeNodes.size() + " nodes, " + (m_queryGraph.edgeSet().size() - prunedQueryGraph.edgeSet().size()) + " edges");
		
		Map<String,Set<String>> bwEdgeSources = new HashMap<String,Set<String>>();
		Map<String,Set<String>> fwEdgeTargets = new HashMap<String,Set<String>>();
		
//		calculateEdgeSets(queryGraph, index, fixedNodes, new HashSet<Integer>(fixedNodes), bwEdgeSources, fwEdgeTargets);
		
		return prunedQueryGraph;
	}
	
	public boolean isRemovedNode(QNode node) {
		return m_removeNodes.contains(node);
	}
	
	public boolean isRemovedNode(String node) {
		return isRemovedNode(new QNode(node));
	}
	
	private void explorePath(QueryGraph g, QNode node, List<QNode> path, Set<QNode> fixedNodes) {
		path.add(node);
		
		if (node.isSelectVariable() || node.isConstant()) {
			for (QNode n : path)
				fixedNodes.add(n);
		}
		
		for (QNode n : g.predecessors(node))
			if (!path.contains(n))
				explorePath(g, n, new ArrayList<QNode>(path), fixedNodes);
		for (QNode n : g.successors(node))
			if (!path.contains(n))
				explorePath(g, n, new ArrayList<QNode>(path), fixedNodes);
	}
}
