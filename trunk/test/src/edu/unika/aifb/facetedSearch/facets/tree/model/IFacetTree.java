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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;

/**
 * @author andi
 * 
 */
public interface IFacetTree {

	public void addLeave2SubtreeRoot(double subtreeRootID, Node leave2add);

	public void addLeaves2SubtreeRoot(double subtreeRootID,
			Collection<? extends Node> leaves2add);

	public List<Node> getChildren(Node father);

	public String getDomain();

	public Node getFather(Node child);

	public double getId();

	public Set<Node> getLeaves4SubtreeRoot(double subtreeRootID);

	public Node getRoot();

	public Node getSubTreeRoot4Node(Node node);

	public Node getVertex(double id);

	public Set<Node> getVertices(int type);

	public boolean hasChildren(Node father);

	public boolean isDirty();

	public boolean isEmpty();

	public void setDomain(String domain);
}