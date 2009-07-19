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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.util.Util;

public class TransformedGraph {
	
	private Map<String,TransformedGraphNode> m_nodeObjects;
	
	public TransformedGraph(QueryGraph queryGraph) {
		m_nodeObjects = new HashMap<String,TransformedGraphNode>();

		for (QNode node : queryGraph.vertexSet()) {
			if(node.isVariable()) {
				TransformedGraphNode tfNode = m_nodeObjects.get(node.getLabel());
				if(tfNode == null) {
					tfNode = new TransformedGraphNode(node, node.getLabel(), TransformedGraphNode.ENTITY_QUERY_NODE);
					m_nodeObjects.put(node.getLabel(), tfNode);
				}
				
				Set<QueryEdge> incomingEdges = queryGraph.incomingEdgesOf(node);
				for (QueryEdge edge : incomingEdges) {
					QNode src = edge.getSource();
					if(Util.isVariable(src.getLabel())) {
						TransformedGraphNode sucTfNode = m_nodeObjects.get(src.getLabel());
						if(sucTfNode == null) {
							sucTfNode = new TransformedGraphNode(src, src.getLabel(), TransformedGraphNode.ENTITY_QUERY_NODE);
							m_nodeObjects.put(src.getLabel(), sucTfNode);
						}
						tfNode.addNeighbor(sucTfNode);
						sucTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(src.getLabel())) {
						if(edge.getLabel().equals(RDF.TYPE.stringValue()) || edge.getLabel().equals("type")) {
							TransformedGraphNode sucTfNode = m_nodeObjects.get(src.getLabel());
							if (sucTfNode == null) {
								sucTfNode = new TransformedGraphNode(src, src.getLabel(), TransformedGraphNode.ENTITY_NODE);
								m_nodeObjects.put(src.getLabel(), sucTfNode);
							}
							tfNode.addNeighbor(sucTfNode);
							sucTfNode.addNeighbor(tfNode);
						}
						else {
							tfNode.addTypeQuery(src.getLabel());
						}
					}
					else if(Util.isConstant(src.getLabel())) {
						tfNode.addAttributeQuery(edge.getLabel(), src.getLabel());
					}
				}
				
				Set<QueryEdge> outgoingEdges = queryGraph.outgoingEdgesOf(node);
				for (QueryEdge edge : incomingEdges) {
					QNode trg = edge.getTarget();
					if(Util.isVariable(trg.getLabel())) {
						TransformedGraphNode predTfNode = m_nodeObjects.get(trg.getLabel());
						if(predTfNode == null) {
							predTfNode = new TransformedGraphNode(trg, trg.getLabel(), TransformedGraphNode.ENTITY_QUERY_NODE);
							m_nodeObjects.put(trg.getLabel(), predTfNode);
						}
						tfNode.addNeighbor(predTfNode);
						predTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(trg.getLabel())) {
						TransformedGraphNode predTfNode = m_nodeObjects.get(trg.getLabel());
						if(predTfNode == null) {
							predTfNode = new TransformedGraphNode(trg, trg.getLabel(), TransformedGraphNode.ENTITY_NODE);
							m_nodeObjects.put(trg.getLabel(), predTfNode);
						}
						tfNode.addNeighbor(predTfNode);
						predTfNode.addNeighbor(tfNode);
					}
				}
			}
		}
		
		for(TransformedGraphNode node : getNodes()) {
			computeDistances(node);
		}
	}
	
	public void computeDistances(TransformedGraphNode startNode) {
		HashSet<TransformedGraphNode> visitedNodes = new HashSet<TransformedGraphNode>(); 
		HashSet<TransformedGraphNode> currentLayer = new HashSet<TransformedGraphNode>();
		HashSet<TransformedGraphNode> nextLayer = new HashSet<TransformedGraphNode>();
		currentLayer.add(startNode);
		
		int dis = 1;
		while(!currentLayer.isEmpty()) {
			for(TransformedGraphNode node : currentLayer) {
				Collection<TransformedGraphNode> neighbors = node.getNeighbors();
				if(neighbors != null && neighbors.size() != 0) {
					for(TransformedGraphNode neighbor : neighbors) {
						if(!visitedNodes.contains(neighbor) && !neighbor.equals(startNode)) {
							startNode.setDistance(neighbor.getNodeId(), dis);
							visitedNodes.add(neighbor);
							nextLayer.add(neighbor);
						}
					}
				}	
			}
			currentLayer = nextLayer;
			nextLayer = new HashSet<TransformedGraphNode>();
			dis++;
		}
	}
	
	public TransformedGraphNode getNode(String label) {
		return m_nodeObjects.get(label);
	}
	
	public Collection<TransformedGraphNode> getNodes() {
		return m_nodeObjects.values();
	}
	
	public Collection<String> getNodeNames() {
		return m_nodeObjects.keySet();
	}

}
