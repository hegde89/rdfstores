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
package edu.unika.aifb.facetedSearch.facets.model;

import java.io.Serializable;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.model.impl.Node;

/**
 * @author andi
 * 
 */
public interface INode extends Serializable {

	public void addFVExtension(String extension);

	public List<IFacetValueTuple> getChildren();

	public List<IFacetValueTuple> getChildren(boolean rankingEnabled);

	public Node getFather();

	public List<String> getFVExtensions();

	public double getID();

	public String getSourceExtension();

	public FacetTree<Node, Edge> getTree();

	public void setFVExtensions(List<String> extensions);

	public void setID(double id);

	public void setSourceExtension(String extension);
}
