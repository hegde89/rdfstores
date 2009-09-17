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
package edu.unika.aifb.facetedSearch.facets.tree.model;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.jgrapht.GraphPath;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree.EndPointType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public interface IFacetTree {

	// public Set<Node> getInnerNodes();
	//
	// public Set<Node> getLeaves();

	public void addEndPoint(EndPointType type, Node endpoint);

	public Queue<Edge> getAncestorPath2RangeRoot(double fromNodeId);

	// public Node getRoot();

	public Queue<Edge> getAncestorPath2Root(double fromNodeId);

	public String getDomain();

	public HashSet<Node> getEndPoints(EndPointType type);

	public double getId();

	public Node getNodeById(double id);

	public Set<Node> getNodesByType(NodeType type);

	public GraphPath<Node, Edge> getPath(Node fromNode, Node toNode);

	public Node getRoot();

	public boolean isEmpty();

	public void setDomain(String domain);

	// public void setEndPoints(HashSet<Node> endPoints);

}