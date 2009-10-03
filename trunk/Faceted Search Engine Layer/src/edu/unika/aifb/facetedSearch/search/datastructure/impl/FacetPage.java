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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.Serializable;
import java.util.Map.Entry;

import org.apache.commons.collections.OrderedMapIterator;
import org.apache.commons.collections.map.LinkedMap;

import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList;
import edu.unika.aifb.facetedSearch.search.datastructure.IFacetPage;

/**
 * @author andi
 * 
 */
public class FacetPage implements IFacetPage, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8092068693656342589L;

	/*
	 * 
	 */
	private Int2ObjectOpenHashMap<String> m_domainHash2domainStringMap;
	private Int2ObjectOpenHashMap<LinkedMap> m_facet2FacetValuesLists;

	public FacetPage() {

		m_domainHash2domainStringMap = new Int2ObjectOpenHashMap<String>();
		m_facet2FacetValuesLists = new Int2ObjectOpenHashMap<LinkedMap>();
	}

	public void addDomain(String domain) {

		if (!m_facet2FacetValuesLists.containsKey(domain)) {

			m_domainHash2domainStringMap.put(domain.hashCode(), domain);
			m_facet2FacetValuesLists.put(domain.hashCode(), new LinkedMap());
		}
	}

	public ObjectIterator<Entry<Integer, LinkedMap>> getDomainEntryIterator() {
		return m_facet2FacetValuesLists.entrySet().iterator();
	}

	public ObjectIterator<String> getDomainIterator() {
		return m_domainHash2domainStringMap.values().iterator();
	}

	public ObjectCollection<String> getDomains() {
		return m_domainHash2domainStringMap.values();
	}

	public OrderedMapIterator getFacetEntryIterator(String domain) {
		return m_facet2FacetValuesLists.get(domain.hashCode())
				.orderedMapIterator();
	}

	public FacetFacetValueList getFacetFacetValuesList(String domain,
			String facet) {

		return (FacetFacetValueList) m_facet2FacetValuesLists.get(
				domain.hashCode()).get(facet);
	}

	public boolean hasDomain(String domain) {
		return m_facet2FacetValuesLists.containsKey(domain.hashCode());
	}

	public boolean put(String domain, String facet, FacetFacetValueList list) {

		if (!hasDomain(domain)) {
			addDomain(domain);
		}

		LinkedMap linkedMap = m_facet2FacetValuesLists.get(domain.hashCode());

		return linkedMap.put(facet, list) == null;
	}
}
