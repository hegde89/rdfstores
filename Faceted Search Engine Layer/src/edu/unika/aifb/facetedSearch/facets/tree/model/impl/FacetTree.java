/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.event.ConnectedComponentTraversalEvent;
import org.jgrapht.event.EdgeTraversalEvent;
import org.jgrapht.event.TraversalListener;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;

import edu.unika.aifb.facetedSearch.facets.tree.model.IFacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public class FacetTree extends DefaultDirectedGraph<Node, Edge> implements
		IFacetTree {

	public enum EndPointType {
		DATA_PROPERTY, OBJECT_PROPERTY, RDF_PROPERTY
	}

	public class FacetTreeTraversalListener implements
			TraversalListener<Node, Edge> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.jgrapht.event.TraversalListener#connectedComponentFinished(org
		 * .jgrapht.event.ConnectedComponentTraversalEvent)
		 */
		public void connectedComponentFinished(
				ConnectedComponentTraversalEvent arg0) {
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.jgrapht.event.TraversalListener#connectedComponentStarted(org
		 * .jgrapht.event.ConnectedComponentTraversalEvent)
		 */
		public void connectedComponentStarted(
				ConnectedComponentTraversalEvent arg0) {
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.jgrapht.event.TraversalListener#edgeTraversed(org.jgrapht.event
		 * .EdgeTraversalEvent)
		 */
		public void edgeTraversed(EdgeTraversalEvent<Node, Edge> arg0) {
			System.out.println();
			System.out.println("Edge: " + arg0.getEdge());
			System.out.println();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.jgrapht.event.TraversalListener#vertexFinished(org.jgrapht.event
		 * .VertexTraversalEvent)
		 */
		public void vertexFinished(VertexTraversalEvent<Node> arg0) {
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.jgrapht.event.TraversalListener#vertexTraversed(org.jgrapht.event
		 * .VertexTraversalEvent)
		 */
		public void vertexTraversed(VertexTraversalEvent<Node> arg0) {

			System.out.println();
			System.out.println("Node: " + arg0.getVertex());
			System.out.println();
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 5087213640961764186L;
	private static Logger s_log = Logger.getLogger(FacetTree.class);

	private double m_id;
	private double m_fatherNodeId;
	private StaticNode m_root;
	private String m_domain;
	private HashMap<EndPointType, HashSet<StaticNode>> m_endPoints;
	private HashSet<Double> m_allEndPoints;
	private HashMap<Double, Node> m_nodeMap;

	public FacetTree() {

		super(Edge.class);
		init();
	}

	public void addEndPoint(EndPointType type, StaticNode endpoint) {
		m_endPoints.get(type).add(endpoint);
		m_allEndPoints.add(endpoint.getID());
	}

	@Override
	public boolean addVertex(Node node) {

		m_nodeMap.put(node.getID(), node);
		return super.addVertex(node);
	}

	@Override
	public boolean containsVertex(Node node) {
		return m_nodeMap.containsKey(node.getID());
	}

	public LinkedList<Edge> getAncestorPath2RangeRoot(double fromNodeId) {

		Node fromNode;
		LinkedList<Edge> edges2RangeRoot = null;

		if ((fromNode = m_nodeMap.get(fromNodeId)) != null) {

			edges2RangeRoot = new LinkedList<Edge>();
			boolean reachedRangeRoot = fromNode.isRangeRoot();
			Node currentNode = fromNode;

			while (!reachedRangeRoot) {

				Iterator<Edge> incomingEdgesIter = incomingEdgesOf(currentNode)
						.iterator();

				if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					edges2RangeRoot.add(edge2father);
					Node father = getEdgeSource(edge2father);

					if (father.isRangeRoot()) {
						reachedRangeRoot = true;
					} else {
						currentNode = father;
					}
				} else {
					s_log.error("tree structure is not correct: " + this);
					break;
				}
			}
		} else {
			s_log.error("node with id " + fromNodeId
					+ " not contained in tree!");
		}

		return edges2RangeRoot;
	}

	// public Set<Node> getInnerNodes() {
	//
	// HashSet<Node> innerNodes = new HashSet<Node>();
	// Iterator<Node> nodesIter = this.vertexSet().iterator();
	//
	// while (nodesIter.hasNext()) {
	//
	// Node node = nodesIter.next();
	//
	// if (this.outDegreeOf(node) != 0) {
	// innerNodes.add(node);
	// }
	// }
	//
	// innerNodes.remove(m_root);
	//
	// return innerNodes;
	// }

	// public Set<Node> getLeaves() {
	//
	// HashSet<Node> leaves = new HashSet<Node>();
	// Iterator<Node> nodesIter = this.vertexSet().iterator();
	//
	// while (nodesIter.hasNext()) {
	//
	// Node node = nodesIter.next();
	//
	// if (this.outDegreeOf(node) == 0) {
	// leaves.add(node);
	// }
	// }
	//
	// return leaves;
	// }

	/**
	 * @return path to root
	 */
	public LinkedList<Edge> getAncestorPath2Root(double fromNodeId) {

		Node fromNode;
		LinkedList<Edge> edges2root = null;

		if ((fromNode = getNodeById(fromNodeId)) != null) {

			edges2root = new LinkedList<Edge>();

			new ArrayList<Edge>();
			boolean reachedRoot = fromNode.isRoot();
			Node currentNode = fromNode;

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = incomingEdgesOf(currentNode)
						.iterator();

				if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					edges2root.add(edge2father);
					Node father = getEdgeSource(edge2father);

					if (father.isRoot()) {
						reachedRoot = true;
					} else {
						currentNode = father;
					}
				} else {
					s_log.error("tree structure is not correct " + this);
					break;
				}
			}
		} else {
			s_log.error("node with id " + fromNodeId
					+ " not contained in tree!");
		}

		return edges2root;
	}

	public String getDomain() {
		return m_domain;
	}

	public HashSet<StaticNode> getEndPoints(EndPointType type) {
		return m_endPoints.get(type);
	}

	public double getFatherNodeId() {
		return m_fatherNodeId;
	}

	// public Node getRoot() {
	// return this.m_root;
	// }

	public double getId() {
		return m_id;
	}

	public Node getNodeById(double key) {

		// Iterator<Node> iter = this.vertexSet().iterator();
		// Node node = null;
		//
		// while (iter.hasNext()) {
		//
		// if ((node = iter.next()).getID() == id) {
		// break;
		// } else {
		// node = null;
		// }
		// }

		return m_nodeMap.get(key);
	}

	public Set<Node> getNodesByType(NodeType type) {

		HashSet<Node> nodes = new HashSet<Node>();

		switch (type) {

		case ROOT: {

			nodes.add(m_root);
			return nodes;
		}
			// case ENDPOINT: {
			//
			// Iterator<Node> iter = this.vertexSet().iterator();
			// Node node = null;
			//
			// while (iter.hasNext()) {
			//
			// node = iter.next();
			//
			// if (((INode) node).getTypes().contains(NodeType.ENDPOINT)) {
			// nodes.add(node);
			// }
			// }
			//
			// return nodes;
			// }
		case INNER_NODE: {

			Iterator<Node> nodesIter = vertexSet().iterator();

			while (nodesIter.hasNext()) {

				Node node = nodesIter.next();

				if (this.outDegreeOf(node) != 0) {
					nodes.add(node);
				}
			}

			nodes.remove(m_root);

			return nodes;
		}
		case LEAVE: {

			Iterator<Node> nodesIter = vertexSet().iterator();

			while (nodesIter.hasNext()) {

				Node node = nodesIter.next();

				if (outDegreeOf(node) == 0) {
					nodes.add(node);
				}
			}

			return nodes;
		}
		case RANGE_ROOT: {

			Iterator<Node> iter = vertexSet().iterator();
			Node node = null;

			while (iter.hasNext()) {

				node = iter.next();

				if (((INode) node).getType() == NodeType.RANGE_ROOT) {
					nodes.add(node);
				}
			}

			return nodes;
		}
		default:
			return null;
		}
	}

	public GraphPath<Node, Edge> getPath(Node fromNode, Node toNode) {

		List<GraphPath<Node, Edge>> paths = (new KShortestPaths<Node, Edge>(
				this, fromNode, 1)).getPaths(toNode);
		return paths.size() > 0 ? paths.get(0) : null;
	}

	public StaticNode getRoot() {
		return m_root;
	}

	private void init() {

		m_nodeMap = new HashMap<Double, Node>();
		m_endPoints = new HashMap<EndPointType, HashSet<StaticNode>>();
		m_allEndPoints = new HashSet<Double>();

		m_root = new StaticNode("root", NodeType.ROOT);
		m_root.setPathHashValue("root".hashCode());
		m_root.setPath("root");
		m_root.setDepth(0);

		addVertex(m_root);

		m_id = (new Random()).nextGaussian();

		m_endPoints.put(EndPointType.DATA_PROPERTY, new HashSet<StaticNode>());
		m_endPoints
				.put(EndPointType.OBJECT_PROPERTY, new HashSet<StaticNode>());
		m_endPoints.put(EndPointType.RDF_PROPERTY, new HashSet<StaticNode>());

	}

	public boolean isEmpty() {
		return this.vertexSet().size() > 1 ? false : true;
	}

	public boolean isEndPoint(Node node) {
		return m_allEndPoints.contains(node.getID());
	}

	public void removeEndPoint(EndPointType type, Node endpoint) {
		m_endPoints.get(type).remove(endpoint);
		m_allEndPoints.remove(endpoint);
	}

	@Override
	public boolean removeVertex(Node node) {

		m_nodeMap.remove(node.getID());
		return super.removeVertex(node);
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setFatherNodeId(double fatherNodeId) {
		m_fatherNodeId = fatherNodeId;
	}

	@Override
	public String toString() {

		DepthFirstIterator<Node, Edge> depthFirstIter = new DepthFirstIterator<Node, Edge>(
				this, getRoot());

		depthFirstIter.addTraversalListener(new FacetTreeTraversalListener());

		// String out = "Nodes: ";
		// out += FacetHelper.NEW_LINE;
		// out += "------------------------------";
		// out += FacetHelper.NEW_LINE;
		// out += FacetHelper.NEW_LINE;

		while (depthFirstIter.hasNext()) {

			depthFirstIter.next();

			// out += depthFirstIter.next().toString();
			//
			// if (depthFirstIter.hasNext()) {
			// out += FacetHelper.NEW_LINE;
			// }
		}

		// out += FacetHelper.NEW_LINE;
		// out += "------------------------------";
		// out += FacetHelper.NEW_LINE;

		return null;
	}
}