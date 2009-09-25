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
package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;

/**
 * @author andi
 * 
 */
public class FacetPage {

	private HashMap<String, FacetFacetValueList> m_facetFacetValuesLists;

	public FacetPage() {
		m_facetFacetValuesLists = new HashMap<String, FacetFacetValueList>();
	}

	public void addFacetFacetValueTuple(String domain,
			FacetFacetValueTuple tuple) {

		FacetFacetValueList list = null;

		if ((list = m_facetFacetValuesLists.get(domain)) == null) {

			list = new FacetFacetValueList();
			list.setFacet(tuple.getFacet());
		}

		list.addFacetFacetValueTuple(tuple);
		m_facetFacetValuesLists.put(domain, list);
	}

	public void put(String domain, FacetFacetValueList list) {
		m_facetFacetValuesLists.put(domain, list);
	}

	public Iterator<Entry<String, FacetFacetValueList>> getEntryIterator() {
		return m_facetFacetValuesLists.entrySet().iterator();
	}

	public void setFacetFacetValuesLists(
			HashMap<String, FacetFacetValueList> facetFacetValuesLists) {
		m_facetFacetValuesLists = facetFacetValuesLists;
	}

	public HashMap<String, FacetFacetValueList> getFacetFacetValuesLists() {
		return m_facetFacetValuesLists;
	}

}
