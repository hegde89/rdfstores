package edu.unika.aifb.keywordsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.impl.Entity;

public class TransformedGraph {
	
	private Map<String,TransformedGraphNode> m_nodeObjects;
	
	public TransformedGraph(Graph<QueryNode> queryGraph) {
		m_nodeObjects = new HashMap<String,TransformedGraphNode>();

		Object[] os = queryGraph.nodes();
		QueryNode[] nodes = new QueryNode [os.length];
		int length = nodes.length;
		for (int i = 0; i < length; i++) {
			nodes[i] = (QueryNode)os[i];
		}
		
		for (int i = 0; i < length; i++) {
			if(Util.isVariable(nodes[i].getName())) {
				TransformedGraphNode tfNode = m_nodeObjects.get(i);
				if(tfNode == null) {
					tfNode = new TransformedGraphNode(i, nodes[i].getName(), TransformedGraphNode.ENTITY_QUERY_NODE);
					m_nodeObjects.put(nodes[i].getName(), tfNode);
				}
				Map<Integer,List<GraphEdge<QueryNode>>> succEdges = queryGraph.successorEdges(i);
				for(Integer suc : succEdges.keySet()) {
					if(Util.isVariable(nodes[suc].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObjects.get(suc);
						if(sucTfNode == null) {
							sucTfNode = new TransformedGraphNode(suc, nodes[suc].getName(), TransformedGraphNode.ENTITY_QUERY_NODE);
							m_nodeObjects.put(nodes[suc].getName(), sucTfNode);
						}
						tfNode.addNeighbor(sucTfNode);
						sucTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(nodes[suc].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObjects.get(suc);
						if(sucTfNode == null) {
							boolean isConcept = false;
							for(GraphEdge<QueryNode> edge : succEdges.get(suc)) {
								if(edge.getLabel().equals(RDF.TYPE.stringValue()))
									isConcept = true;
							}
							if(isConcept == false) {
								sucTfNode = new TransformedGraphNode(suc, nodes[suc].getName(), TransformedGraphNode.ENTITY_NODE);
								m_nodeObjects.put(nodes[suc].getName(), sucTfNode);
							}
						}
						tfNode.addNeighbor(sucTfNode);
						sucTfNode.addNeighbor(tfNode);
					}
					else if(Util.isConstant(nodes[suc].getName())) {
						for(GraphEdge<QueryNode> edge : succEdges.get(suc)) {
							tfNode.addAttributeQuery(edge.getLabel(), nodes[suc].getName());
						}
					}
				}
				
				Map<Integer,List<GraphEdge<QueryNode>>> predEdges = queryGraph.predecessorEdges(i);
				for(Integer pred : predEdges.keySet()) {
					if(Util.isVariable(nodes[pred].getName())) {
						TransformedGraphNode predTfNode = m_nodeObjects.get(pred);
						if(predTfNode == null) {
							predTfNode = new TransformedGraphNode(pred, nodes[pred].getName(), TransformedGraphNode.ENTITY_QUERY_NODE);
							m_nodeObjects.put(nodes[pred].getName(), predTfNode);
						}
						tfNode.addNeighbor(predTfNode);
						predTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(nodes[pred].getName())) {
						TransformedGraphNode predTfNode = m_nodeObjects.get(pred);
						if(predTfNode == null) {
							predTfNode = new TransformedGraphNode(pred, nodes[pred].getName(), TransformedGraphNode.ENTITY_NODE);
							m_nodeObjects.put(nodes[pred].getName(), predTfNode);
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
	
	public Collection<TransformedGraphNode> getNodes() {
		return m_nodeObjects.values();
	}
	
	public Collection<String> getNodeNames() {
		return m_nodeObjects.keySet();
	}

}
