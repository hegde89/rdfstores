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
package edu.unika.aifb.facetedSearch.facets.model.impl;

import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.tree.model.IEdge;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;

/**
 * @author andi
 * 
 */
public class FacetValueTuple implements IFacetValueTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7078149934666105106L;
	private INode m_value;
	private IEdge m_facet;

	public FacetValueTuple(IEdge edge, INode node) {
		this.m_value = node;
		this.m_facet = edge;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValueTuple#getFacet()
	 */
	public IEdge getFacet() {
		return this.m_facet;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValueTuple#getValue()
	 */
	public INode getValue() {
		return this.m_value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.api.IFacetValueTuple#setFacet(edu
	 * .unika.aifb.facetedSearch.facets.api.IEdge)
	 */
	public void setFacet(IEdge edge) {
		this.m_facet = edge;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.api.IFacetValueTuple#setValue(edu
	 * .unika.aifb.facetedSearch.facets.api.INode)
	 */
	public void setValue(INode node) {
		this.m_value = node;
	}
}
