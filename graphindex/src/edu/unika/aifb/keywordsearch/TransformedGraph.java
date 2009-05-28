package edu.unika.aifb.keywordsearch;

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
	
	private Map<Integer,TransformedGraphNode> m_nodeObjects;
	private Map<String, Integer> m_name2id;
	private Set<String> m_nodesWithNoEntities;
	
	public TransformedGraph(Graph<QueryNode> queryGraph) {
		m_nodeObjects = new HashMap<Integer,TransformedGraphNode>();
		m_name2id = new HashMap<String, Integer>();
		m_nodesWithNoEntities = new HashSet<String>();

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
					m_nodeObjects.put(i, tfNode);
				}
				Map<Integer,List<GraphEdge<QueryNode>>> succEdges = queryGraph.successorEdges(i);
				for(Integer suc : succEdges.keySet()) {
					if(Util.isVariable(nodes[suc].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObjects.get(suc);
						if(sucTfNode == null) {
							sucTfNode = new TransformedGraphNode(suc, nodes[suc].getName(), TransformedGraphNode.ENTITY_QUERY_NODE);
							m_nodeObjects.put(suc, sucTfNode);
						}
						tfNode.addNeighbor(sucTfNode);
						sucTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(nodes[suc].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObjects.get(suc);
						if(sucTfNode == null) {
							if(succEdges.get(suc).iterator().next().getLabel().equals(RDF.TYPE.stringValue())) {
								sucTfNode = new TransformedGraphNode(suc, nodes[suc].getName(), TransformedGraphNode.CONCEPT_NODE);
								m_nodeObjects.put(suc, sucTfNode);
							}
							else {
								sucTfNode = new TransformedGraphNode(suc, nodes[suc].getName(), TransformedGraphNode.ENTITY_NODE);
								m_nodeObjects.put(suc, sucTfNode);
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
							m_nodeObjects.put(pred, predTfNode);
						}
						tfNode.addNeighbor(predTfNode);
						predTfNode.addNeighbor(tfNode);
					}	
					else if(Util.isEntity(nodes[pred].getName())) {
						TransformedGraphNode predTfNode = m_nodeObjects.get(pred);
						if(predTfNode == null) {
							predTfNode = new TransformedGraphNode(pred, nodes[pred].getName(), TransformedGraphNode.ENTITY_NODE);
							m_nodeObjects.put(pred, predTfNode);
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
	
	public TransformedGraphNode computeCentricNode() {
		int minDistance = 0;
		int minNumOfEntities = 0;
		TransformedGraphNode centricNode = null; 
		for(TransformedGraphNode node : getNodes()) {
			int size = node.getNumOfEntities();
			if(size != 0) {
				if(node.getMaxDistance() < minDistance || minDistance == 0) {
					minDistance = node.getMaxDistance();
					minNumOfEntities = size; 
					centricNode = node; 
				}
				else if(node.getMaxDistance() == minDistance) {
					if(size < minNumOfEntities || minNumOfEntities == 0) {
						minNumOfEntities = size;
						centricNode = node; 
					}
				}
			}
			else {
				m_nodesWithNoEntities.add(node.getNodeName());
			}
		}
		
		return centricNode;
	}
	
	public GTable<Entity> approximateStructureMatching() {
		TransformedGraphNode startNode = computeCentricNode();
		if(startNode == null)
			return null;
		
		
		
		return null;
		
	}

}
