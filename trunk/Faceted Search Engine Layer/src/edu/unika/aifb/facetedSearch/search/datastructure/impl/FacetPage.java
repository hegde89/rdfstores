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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections.OrderedMapIterator;
import org.apache.commons.collections.map.LinkedMap;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.search.datastructure.IFacetPage;

/**
 * @author andi
 * 
 */
public class FacetPage implements IFacetPage {

	private HashMap<String, LinkedMap> m_facetFacetValuesLists;

	public FacetPage() {
		m_facetFacetValuesLists = new HashMap<String, LinkedMap>();
	}

	public void addDomain(String domain) {

		if (!m_facetFacetValuesLists.containsKey(domain)) {
			m_facetFacetValuesLists.put(domain, new LinkedMap());
		}
	}

	public void addFacetFacetValueTuple(String domain,
			FacetFacetValueTuple tuple) {

		LinkedMap linkedMap;

		if ((linkedMap = m_facetFacetValuesLists.get(domain)) == null) {
			linkedMap = new LinkedMap();
		}

		FacetFacetValueList facetFacetValueList;

		if ((facetFacetValueList = (FacetFacetValueList) linkedMap.get(tuple
				.getFacet())) == null) {
			facetFacetValueList = new FacetFacetValueList();
		}

		facetFacetValueList.addFacetFacetValueTuple(tuple);
		linkedMap.put(tuple.getFacet(), facetFacetValueList);

		m_facetFacetValuesLists.put(domain, linkedMap);
	}

	public Iterator<Entry<String, LinkedMap>> getDomainEntryIterator() {
		return m_facetFacetValuesLists.entrySet().iterator();
	}

	public Set<String> getDomains() {
		return m_facetFacetValuesLists.keySet();
	}

	public OrderedMapIterator getFacetEntryIterator(String domain) {
		return m_facetFacetValuesLists.get(domain).orderedMapIterator();
	}

	public FacetFacetValueList getFacetFacetValuesList(String domain,
			String facet) {
		return (FacetFacetValueList) m_facetFacetValuesLists.get(domain).get(
				facet);
	}

	public void put(String domain, String facet, FacetFacetValueList list) {

		LinkedMap linkedMap = m_facetFacetValuesLists.get(domain);
		linkedMap.put(facet, list);
	}
}
