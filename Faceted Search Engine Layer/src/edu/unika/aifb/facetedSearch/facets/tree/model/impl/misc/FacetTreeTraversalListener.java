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
package edu.unika.aifb.facetedSearch.facets.tree.model.impl.misc;

import org.jgrapht.event.ConnectedComponentTraversalEvent;
import org.jgrapht.event.EdgeTraversalEvent;
import org.jgrapht.event.TraversalListener;
import org.jgrapht.event.VertexTraversalEvent;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;

/**
 * @author andi
 * 
 */
public class FacetTreeTraversalListener
		implements
			TraversalListener<Node, Edge> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.event.TraversalListener#connectedComponentFinished(org
	 * .jgrapht.event.ConnectedComponentTraversalEvent)
	 */
	public void connectedComponentFinished(ConnectedComponentTraversalEvent arg0) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.event.TraversalListener#connectedComponentStarted(org
	 * .jgrapht.event.ConnectedComponentTraversalEvent)
	 */
	public void connectedComponentStarted(ConnectedComponentTraversalEvent arg0) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.event.TraversalListener#edgeTraversed(org.jgrapht.event
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
	 * @see org.jgrapht.event.TraversalListener#vertexFinished(org.jgrapht.event
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
