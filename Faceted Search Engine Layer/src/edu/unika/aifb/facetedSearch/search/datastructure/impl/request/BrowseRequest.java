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
package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;

/**
 * @author andi
 * 
 */
public class BrowseRequest extends AbstractFacetRequest {

	private FacetFacetValueTuple m_tuple;

	public BrowseRequest() {
		super("browseRequest");
	}

	public BrowseRequest(String name) {
		super(name);
	}

	public FacetFacetValueTuple getTuple() {
		return m_tuple;
	}

	public void setTuple(FacetFacetValueTuple tuple) {
		m_tuple = tuple;
	}
}
