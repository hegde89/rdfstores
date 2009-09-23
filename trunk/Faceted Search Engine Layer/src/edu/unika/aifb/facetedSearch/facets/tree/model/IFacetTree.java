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
import java.util.LinkedList;
import java.util.Set;

import org.jgrapht.GraphPath;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree.EndPointType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;

/**
 * @author andi
 * 
 */
public interface IFacetTree {

	public void addEndPoint(EndPointType type, StaticNode endpoint);

	public LinkedList<Edge> getAncestorPath2RangeRoot(double fromNodeId);

	// public Set<Node> getInnerNodes();
	//
	// public Set<Node> getLeaves();

	public LinkedList<Edge> getAncestorPath2Root(double fromNodeId);

	public String getDomain();

	public HashSet<StaticNode> getEndPoints(EndPointType type);

	// public Node getRoot();

	public double getFatherNodeId();

	public double getId();

	public Node getNodeById(double id);

	public Set<Node> getNodesByType(NodeType type);

	public GraphPath<Node, Edge> getPath(Node fromNode, Node toNode);

	public Node getRoot();

	public boolean isEmpty();

	public boolean isEndPoint(Node node);

	public void removeEndPoint(EndPointType type, Node endpoint);

	public void setDomain(String domain);

	public void setFatherNodeId(double fatherNodeId);

	// public void setEndPoints(HashSet<Node> endPoints);

}