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

import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;

import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.IFacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.misc.FacetTreeTraversalListener;

/**
 * @author andi
 * 
 */
public class FacetTree extends DefaultDirectedGraph<Node, Edge>
		implements
			IFacetTree,
			Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5087213640961764186L;

	/*
	 * 
	 */
	private static final String ROOT = "root";

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(FacetTree.class);

	/*
	 * 
	 */
	private double m_id;
	private StaticNode m_root;
	private String m_domain;
	private boolean m_dirty;

	/*
	 * 
	 */
	private Double2ObjectOpenHashMap<Node> m_nodeMap;
	private Double2ObjectOpenHashMap<Set<Node>> m_nodeTypeMap;
	private Double2ObjectOpenHashMap<Set<Node>> m_subtreeRoot2LeavesMap;

	public FacetTree() {

		super(Edge.class);
		init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.graph.AbstractBaseGraph#addEdge(java.lang.Object,
	 * java.lang.Object)
	 */
	@Override
	public Edge addEdge(Node arg0, Node arg1) {

		m_dirty = true;

		Edge edge = super.addEdge(arg0, arg1);
		edge.setSource(arg0);
		edge.setTarget(arg1);

		return edge;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.graph.AbstractBaseGraph#addEdge(java.lang.Object,
	 * java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean addEdge(Node arg0, Node arg1, Edge arg2) {

		m_dirty = true;

		arg2.setSource(arg0);
		arg2.setTarget(arg1);

		return super.addEdge(arg0, arg1, arg2);
	}

	public void addLeave2SubtreeRoot(double subtreeRootID, Node leave2add) {

		Set<Node> leaves;

		if ((leaves = m_subtreeRoot2LeavesMap.get(subtreeRootID)) == null) {
			leaves = new HashSet<Node>();
		}

		leaves.add(leave2add);
		m_subtreeRoot2LeavesMap.put(subtreeRootID, leaves);
	}

	public void addLeaves2SubtreeRoot(double subtreeRootID,
			Collection<? extends Node> leaves2add) {

		Set<Node> leaves;

		if ((leaves = m_subtreeRoot2LeavesMap.get(subtreeRootID)) == null) {
			leaves = new HashSet<Node>();
		}

		leaves.addAll(leaves2add);
		m_subtreeRoot2LeavesMap.put(subtreeRootID, leaves);
	}

	@Override
	public boolean addVertex(Node node) {

		m_dirty = true;

		m_nodeMap.put(node.getID(), node);
		return super.addVertex(node);
	}

	@Override
	public boolean containsVertex(Node node) {
		return m_nodeMap.containsKey(node.getID());
	}

	public List<Node> getChildren(Node father) {

		List<Node> children = new ArrayList<Node>();
		Iterator<Edge> outEdgesIter = outgoingEdgesOf(father).iterator();

		while (outEdgesIter.hasNext()) {
			children.add(getEdgeTarget(outEdgesIter.next()));
		}

		return children;
	}

	public String getDomain() {
		return m_domain;
	}

	public Node getFather(Node child) {

		Node father = null;

		if (!child.isRoot()) {

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

	public Set<Node> getLeaves4SubtreeRoot(double subtreeRootID) {
		return m_subtreeRoot2LeavesMap.get(subtreeRootID);
	}

	public StaticNode getRoot() {
		return m_root;
	}

	public Node getSubTreeRoot4Node(Node node) {

		Node subTreeRoot = node;

		if (!node.isSubTreeRoot()) {

			boolean reachedSubTreeRoot = false;

			while (!reachedSubTreeRoot) {

				if ((node = getFather(node)).isSubTreeRoot()) {
					reachedSubTreeRoot = true;
				}
			}
		}

		return subTreeRoot;
	}

	public Node getVertex(double nodeID) {
		return m_nodeMap.get(nodeID);
	}

	public Set<Node> getVertices(int type) {

		switch (type) {

			case NodeType.ROOT : {

				HashSet<Node> nodes = new HashSet<Node>();
				nodes.add(m_root);
				return nodes;
			}
			case NodeType.INNER_NODE : {

				if (!m_nodeTypeMap.containsKey(NodeType.INNER_NODE)
						|| isDirty()) {

					m_dirty = false;

					HashSet<Node> nodes = new HashSet<Node>();
					Iterator<Node> nodesIter = vertexSet().iterator();

					while (nodesIter.hasNext()) {

						Node node = nodesIter.next();

						if (this.outDegreeOf(node) != 0) {
							nodes.add(node);
						}
					}

					nodes.remove(m_root);
					m_nodeTypeMap.put(NodeType.INNER_NODE, nodes);
				}

				return m_nodeTypeMap.get(NodeType.INNER_NODE);
			}
			case NodeType.LEAVE : {

				if (!m_nodeTypeMap.containsKey(NodeType.LEAVE) || isDirty()) {

					m_dirty = false;

					HashSet<Node> nodes = new HashSet<Node>();
					Iterator<Node> nodesIter = vertexSet().iterator();

					while (nodesIter.hasNext()) {

						Node node = nodesIter.next();

						if (outDegreeOf(node) == 0) {
							nodes.add(node);
						}
					}

					m_nodeTypeMap.put(NodeType.LEAVE, nodes);
				}

				return m_nodeTypeMap.get(NodeType.LEAVE);
			}
			case NodeType.RANGE_ROOT : {

				if (!m_nodeTypeMap.containsKey(NodeType.RANGE_ROOT)
						|| isDirty()) {

					m_dirty = false;

					HashSet<Node> nodes = new HashSet<Node>();
					Iterator<Node> nodesIter = vertexSet().iterator();

					while (nodesIter.hasNext()) {

						Node node = nodesIter.next();

						if (((INode) node).getType() == NodeType.RANGE_ROOT) {
							nodes.add(node);
						}
					}

					nodes.remove(m_root);
					m_nodeTypeMap.put(NodeType.INNER_NODE, nodes);
				}

				return m_nodeTypeMap.get(NodeType.RANGE_ROOT);
			}
			default :
				return null;
		}
	}

	public boolean hasChildren(Node father) {
		return outgoingEdgesOf(father).iterator().hasNext();
	}

	private void init() {

		/*
		 * 
		 */
		m_id = (new Random()).nextGaussian();
		m_dirty = false;

		/*
		 * 
		 */
		m_nodeMap = new Double2ObjectOpenHashMap<Node>();
		m_nodeTypeMap = new Double2ObjectOpenHashMap<Set<Node>>();
		m_subtreeRoot2LeavesMap = new Double2ObjectOpenHashMap<Set<Node>>();

		/*
		 * 
		 */
		m_root = new StaticNode();
		m_root.setValue(ROOT);
		m_root.setType(NodeType.ROOT);
		m_root.setPath(ROOT);
		m_root.setDepth(0);
		m_root.setIsSubTreeRoot(true);
		m_root.setFacet(Facet.NULL);

		addVertex(m_root);
	}

	public boolean isDirty() {
		return m_dirty;
	}

	public boolean isEmpty() {
		return vertexSet().size() > 1 ? false : true;
	}

	@Override
	public boolean removeVertex(Node node) {

		m_dirty = true;

		m_nodeMap.remove(node.getID());
		return super.removeVertex(node);
	}

	public void setDomain(String domain) {
		m_domain = domain;
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