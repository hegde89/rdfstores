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
package edu.unika.aifb.facetedSearch.facets.tree;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;

/**
 * @author andi
 * 
 */
public interface IFacetTree {

	public void addEndPoint(int epType, StaticNode endpoint);

	public LinkedList<Edge> getAncestorPath2RangeRoot(double fromNodeId);

	public LinkedList<Edge> getAncestorPath2Root(double fromNodeId);

	public List<Node> getChildren(Node father);

	public String getDomain();

	public DoubleOpenHashSet getEndPoints();

	public HashSet<StaticNode> getEndPoints(int type);

	public Node getFather(Node child);

	public double getId();

	public Node getRoot();

	public Node getVertex(double id);

	public Set<Node> getVertex(int type);

	public boolean hasChildren(Node father);

	public boolean isEmpty();

	public boolean isRankingEnabled();

	public void setDomain(String domain);

	public void setRankingEnabled(boolean rankingEnabled);
}