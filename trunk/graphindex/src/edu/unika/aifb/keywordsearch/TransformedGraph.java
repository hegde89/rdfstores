package edu.unika.aifb.keywordsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.util.Util;

public class TransformedGraph {
	
	private Map<Integer,TransformedGraphNode> m_nodeObject;
	private Collection<TransformedGraphNode> m_centricNodes;
	
	public TransformedGraph(Graph<QueryNode> queryGraph) {
		m_nodeObject = new HashMap<Integer,TransformedGraphNode>();
		m_centricNodes = new HashSet<TransformedGraphNode>();

		Object[] os = queryGraph.nodes();
		QueryNode[] nodes = new QueryNode [os.length];
		int length = nodes.length;
		for (int i = 0; i < length; i++) {
			nodes[i] = (QueryNode)os[i];
		}
		
		for (int i = 0; i < length; i++) {
			if(Util.isVariable(nodes[i].getName())) {
				TransformedGraphNode tfNode = m_nodeObject.get(i);
				if(tfNode == null) {
					tfNode = new TransformedGraphNode(i);
					m_nodeObject.put(i, tfNode);
				}
				Map<Integer,List<GraphEdge<QueryNode>>> succEdges = queryGraph.successorEdges(i);
				for(Integer suc : succEdges.keySet()) {
					if(Util.isVariable(nodes[suc].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObject.get(suc);
						if(sucTfNode == null) {
							sucTfNode = new TransformedGraphNode(suc);
							m_nodeObject.put(suc, sucTfNode);
						}
						tfNode.addNeighbor(sucTfNode);
					}	
					else if(Util.isEntity(nodes[suc].getName())) {
						
					}
					else if(Util.isConstant(nodes[suc].getName())) {
						for(GraphEdge<QueryNode> edge : succEdges.get(suc)) {
							tfNode.addEntityQuery(edge.getLabel(), nodes[suc].getName());
						}
					}
				}
				
				Map<Integer,List<GraphEdge<QueryNode>>> predEdges = queryGraph.predecessorEdges(i);
				for(Integer pred : predEdges.keySet()) {
					if(Util.isVariable(nodes[pred].getName())) {
						TransformedGraphNode sucTfNode = m_nodeObject.get(pred);
						if(sucTfNode == null) {
							sucTfNode = new TransformedGraphNode(pred);
							m_nodeObject.put(pred, sucTfNode);
						}
						tfNode.addNeighbor(sucTfNode);
					}	
					else if(Util.isEntity(nodes[pred].getName())) {
						
					}
				}
			}
			else if(Util.isEntity(nodes[i].getName())) {
				
			}
		}
		
		for(TransformedGraphNode node : getNodes()) {
			computeDistances(node);
		}
		
		m_centricNodes = computeCentricNodes();
	}
	
	public Collection<TransformedGraphNode> computeCentricNodes() {
		int min = 0;
		Collection<TransformedGraphNode> centricNodes = new ArrayList<TransformedGraphNode>(); 
		for(TransformedGraphNode node : getNodes()) {
			if(node.getMaxDistance() < min || min == 0) {
				min = node.getMaxDistance();
				centricNodes = new ArrayList<TransformedGraphNode>(); 
				centricNodes.add(node);
			}
			else if(node.getMaxDistance() == min) {
				centricNodes.add(node);
			}
		}
		
		return centricNodes;
	}
	
	public void computeDistances(TransformedGraphNode startNode) {
		HashSet<TransformedGraphNode> reachableNodes = new HashSet<TransformedGraphNode>(); 
		HashSet<TransformedGraphNode> currentLayer = new HashSet<TransformedGraphNode>();
		HashSet<TransformedGraphNode> nextLayer = new HashSet<TransformedGraphNode>();
		currentLayer.add(startNode);
		
		int dis = 1;
		while(!currentLayer.isEmpty()) {
			for(TransformedGraphNode node : currentLayer) {
				Collection<TransformedGraphNode> neighbors = node.getNeighbors();
				if(neighbors != null && neighbors.size() != 0) {
					for(TransformedGraphNode neighbor : neighbors) {
						if(!reachableNodes.contains(neighbor) && !neighbor.equals(startNode)) {
							startNode.setDistance(neighbor.getNodeId(), dis);
							reachableNodes.add(neighbor);
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
		return m_nodeObject.values();
	}
	
	public Collection<TransformedGraphNode> getCentricNodes() {
		return m_centricNodes;
	}

}
