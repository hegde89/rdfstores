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
package edu.unika.aifb.facetedSearch.index.tree.model.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jgrapht.event.ConnectedComponentTraversalEvent;
import org.jgrapht.event.EdgeTraversalEvent;
import org.jgrapht.event.TraversalListener;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.traverse.DepthFirstIterator;

import edu.unika.aifb.facetedSearch.index.tree.model.IFacetTree;
import edu.unika.aifb.facetedSearch.index.tree.model.INode;
import edu.unika.aifb.facetedSearch.index.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public class FacetTree extends DefaultDirectedWeightedGraph<Node, Edge>
		implements IFacetTree {

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

	private Node m_root;

	public FacetTree() {

		super(Edge.class);

		this.m_root = new Node("root", NodeType.ROOT);
		this.addVertex(this.m_root);
	}

	public Set<Node> getLeaves() {

		HashSet<Node> leaves = new HashSet<Node>();
		Iterator<Node> nodesIter = this.vertexSet().iterator();

		while (nodesIter.hasNext()) {

			Node node = nodesIter.next();

			if (this.outDegreeOf(node) == 0) {
				leaves.add(node);
			}
		}

		return leaves;
	}

	public Set<Node> getInnerNodes() {

		HashSet<Node> innerNodes = new HashSet<Node>();
		Iterator<Node> nodesIter = this.vertexSet().iterator();

		while (nodesIter.hasNext()) {

			Node node = nodesIter.next();

			if (this.outDegreeOf(node) != 0) {
				innerNodes.add(node);
			}
		}

		innerNodes.remove(m_root);

		return innerNodes;
	}

	public Node getNodeById(double id) {

		Iterator<Node> iter = this.vertexSet().iterator();
		Node node = null;

		while (iter.hasNext()) {

			node = iter.next();

			if (((INode) node).getID() == id) {
				break;
			} else {
				node = null;
			}
		}

		return null;
	}

	public Node getRoot() {
		return this.m_root;
	}

	public boolean isEmpty() {
		return this.vertexSet().size() > 1 ? false : true;
	}

	@Override
	public String toString() {

		DepthFirstIterator<Node, Edge> depthFirstIter = new DepthFirstIterator<Node, Edge>(
				this, this.getRoot());

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