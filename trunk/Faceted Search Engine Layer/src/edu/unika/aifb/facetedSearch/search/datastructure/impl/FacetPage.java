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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
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

	/*
	 * 
	 */
	private Int2ObjectOpenHashMap<List<FacetFacetValueList>> m_facet2FacetValuesLists;

	public FacetPage() {

		m_domainHash2domainStringMap = new Int2ObjectOpenHashMap<String>();
		m_facet2FacetValuesLists = new Int2ObjectOpenHashMap<List<FacetFacetValueList>>();
	}

	public void addDomain(String domain) {

		if (!m_facet2FacetValuesLists.containsKey(domain.hashCode())) {

			m_domainHash2domainStringMap.put(domain.hashCode(), domain);
			m_facet2FacetValuesLists.put(domain.hashCode(),
					new ArrayList<FacetFacetValueList>());
		}
	}

	public ObjectIterator<Entry<Integer, List<FacetFacetValueList>>> getDomainEntryIterator() {
		return m_facet2FacetValuesLists.entrySet().iterator();
	}

	public ObjectIterator<String> getDomainIterator() {
		return m_domainHash2domainStringMap.values().iterator();
	}

	public ObjectCollection<String> getDomains() {
		return m_domainHash2domainStringMap.values();
	}

	public Iterator<FacetFacetValueList> getFacetFacetValueListIterator(
			String domain) {
		return m_facet2FacetValuesLists.get(domain.hashCode()).iterator();
	}

	public FacetFacetValueList getFacetFacetValuesList(String domain,
			String facetUri) {

		FacetFacetValueList out = null;
		Iterator<FacetFacetValueList> fvListIter = getFacetFacetValueListIterator(domain);

		FacetFacetValueList fvList;

		while (fvListIter.hasNext()) {

			if ((fvList = fvListIter.next()).getFacet().getUri().equals(
					facetUri)) {
				out = fvList;
				break;
			}
		}

		return out;
	}

	public boolean hasDomain(String domain) {
		return m_facet2FacetValuesLists.containsKey(domain.hashCode());
	}

	public boolean put(String domain, Facet facet, FacetFacetValueList list) {

		if (!hasDomain(domain)) {
			addDomain(domain);
		}

		return m_facet2FacetValuesLists.get(domain.hashCode()).add(list);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		String out = new String();
		ObjectIterator<Entry<Integer, List<FacetFacetValueList>>> domainEntryIter = getDomainEntryIterator();

		while (domainEntryIter.hasNext()) {

			Entry<Integer, List<FacetFacetValueList>> domainEntry = domainEntryIter
					.next();

			out += FacetEnvironment.DIVIDER;
			out += FacetEnvironment.NEW_LINE;

			out += "domain: "
					+ m_domainHash2domainStringMap.get(domainEntry.getKey());
			out += FacetEnvironment.NEW_LINE;
			
			Iterator<FacetFacetValueList> fvListMapIter = domainEntry
					.getValue().iterator();

			while (fvListMapIter.hasNext()) {

				FacetFacetValueList fvList = fvListMapIter.next();

				out += "facet: " + fvList.getFacet();
				out += FacetEnvironment.NEW_LINE;

				Iterator<Facet> subfacetIter = fvList.getSubFacetIterator();

				while (subfacetIter.hasNext()) {

					out += "> subfacet: " + subfacetIter.next();
					out += FacetEnvironment.NEW_LINE;
				}

				Iterator<AbstractBrowsingObject> historyIter = fvList
						.getHistoryIterator();

				while (historyIter.hasNext()) {

					out += "> history: " + historyIter.next();
					out += FacetEnvironment.NEW_LINE;
				}

				Iterator<AbstractFacetValue> valueIter = fvList
						.getFacetValueIterator();

				while (valueIter.hasNext()) {

					out += "> value: " + valueIter.next();
					out += FacetEnvironment.NEW_LINE;
				}
			}
		}

		return out;
	}
}
