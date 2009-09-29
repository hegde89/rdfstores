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
package edu.unika.aifb.facetedSearch.facets.tree.impl;

import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.tree.IFacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.misc.FacetTreeTraversalListener;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.misc.NodeComparator;

/**
 * @author andi
 * 
 */
public class FacetTree extends DefaultDirectedGraph<Node, Edge>
		implements
			IFacetTree {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5087213640961764186L;
	private static final String ROOT = "root";
	private static Logger s_log = Logger.getLogger(FacetTree.class);

	/*
	 * 
	 */
	private double m_id;
	private StaticNode m_root;
	private String m_domain;
	private boolean m_rankingEnabled;

	/*
	 * 
	 */
	private DoubleOpenHashSet m_allEndPoints;
	private Double2ObjectOpenHashMap<Node> m_nodeMap;
	private Int2ObjectOpenHashMap<HashSet<StaticNode>> m_endPoints;

	/*
	 * 
	 */
	private NodeComparator m_nodeComparator;

	public FacetTree() {

		super(Edge.class);
		init();
	}

	public void addEndPoint(int epType, StaticNode endpoint) {
		m_endPoints.get(epType).add(endpoint);
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

	public LinkedList<Edge> getAncestorPath2Root(double fromNodeId) {

		Node fromNode;
		LinkedList<Edge> edges2root = null;

		if ((fromNode = getVertex(fromNodeId)) != null) {

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

	public List<Node> getChildren(Node father) {

		List<Node> children = new ArrayList<Node>();
		Iterator<Edge> outEdgesIter = outgoingEdgesOf(father).iterator();

		while (outEdgesIter.hasNext()) {

			children.add(getEdgeTarget(outEdgesIter.next()));
		}

		if (isRankingEnabled()) {
			Collections.sort(children, m_nodeComparator);
		}

		return children;
	}

	public String getDomain() {
		return m_domain;
	}

	public DoubleOpenHashSet getEndPoints() {
		return m_allEndPoints;
	}

	public HashSet<StaticNode> getEndPoints(int type) {
		return m_endPoints.get(type);
	}

	public Node getFather(Node child) {

		Node father = null;

		if (!child.equals(getRoot())) {

			Iterator<Edge> inEdgesIter = incomingEdgesOf(child).iterator();

			if (inEdgesIter.hasNext()) {
				father = getEdgeSource(inEdgesIter.next());
			}
		}

		return father;
	}

	public double getId() {
		return m_id;
	}

	public StaticNode getRoot() {
		return m_root;
	}

	public Node getVertex(double nodeID) {
		return m_nodeMap.get(nodeID);
	}

	public Set<Node> getVertex(NodeType type) {

		HashSet<Node> nodes = new HashSet<Node>();

		switch (type) {

			case ROOT : {

				nodes.add(m_root);
				return nodes;
			}
			case INNER_NODE : {

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
			case LEAVE : {

				Iterator<Node> nodesIter = vertexSet().iterator();

				while (nodesIter.hasNext()) {

					Node node = nodesIter.next();

					if (outDegreeOf(node) == 0) {
						nodes.add(node);
					}
				}

				return nodes;
			}
			case RANGE_ROOT : {

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
			default :
				return null;
		}
	}

	public boolean hasChildren(Node father) {
		return outgoingEdgesOf(father).iterator().hasNext();
	}

	private void init() {

		m_nodeMap = new Double2ObjectOpenHashMap<Node>();
		m_endPoints = new Int2ObjectOpenHashMap<HashSet<StaticNode>>();
		m_allEndPoints = new DoubleOpenHashSet();

		m_root = new StaticNode(ROOT, NodeType.ROOT);
		m_root.setPathHashValue(ROOT.hashCode());
		m_root.setPath(ROOT);
		m_root.setDepth(0);

		addVertex(m_root);

		m_id = (new Random()).nextGaussian();

		m_endPoints.put(FacetEnvironment.EndPointType.DATA_PROPERTY,
				new HashSet<StaticNode>());
		m_endPoints.put(FacetEnvironment.EndPointType.OBJECT_PROPERTY,
				new HashSet<StaticNode>());
		m_endPoints.put(FacetEnvironment.EndPointType.RDF_PROPERTY,
				new HashSet<StaticNode>());

		m_nodeComparator = new NodeComparator();
	}

	public boolean isEmpty() {
		return this.vertexSet().size() > 1 ? false : true;
	}

	public boolean isRankingEnabled() {
		return m_rankingEnabled;
	}

	@Override
	public boolean removeVertex(Node node) {

		m_nodeMap.remove(node.getID());
		return super.removeVertex(node);
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setRankingEnabled(boolean rankingEnabled) {
		m_rankingEnabled = rankingEnabled;
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