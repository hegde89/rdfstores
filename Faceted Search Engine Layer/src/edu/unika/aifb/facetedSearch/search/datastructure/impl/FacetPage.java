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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
	private HashMap<String, List<FacetFacetValueList>> m_domain2FacetValuesLists;

	public FacetPage() {

		m_domain2FacetValuesLists = new HashMap<String, List<FacetFacetValueList>>();
	}

	public void addDomain(String domain) {

		if (!m_domain2FacetValuesLists.containsKey(domain)) {

			m_domain2FacetValuesLists.put(domain,
					new ArrayList<FacetFacetValueList>());
		}
	}

	public Iterator<Entry<String, List<FacetFacetValueList>>> getDomainEntryIterator() {
		return m_domain2FacetValuesLists.entrySet().iterator();
	}

	public Iterator<String> getDomainIterator() {
		return m_domain2FacetValuesLists.keySet().iterator();
	}

	public Set<String> getDomains() {
		return m_domain2FacetValuesLists.keySet();
	}

	public Iterator<FacetFacetValueList> getFacetFacetValueListIterator(
			String domain) {
		return m_domain2FacetValuesLists.get(domain).iterator();
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
		return m_domain2FacetValuesLists.containsKey(domain);
	}

	public boolean put(String domain, Facet facet, FacetFacetValueList list) {

		if (!hasDomain(domain)) {
			addDomain(domain);
		}

		return m_domain2FacetValuesLists.get(domain).add(list);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		String out = new String();
		Iterator<Entry<String, List<FacetFacetValueList>>> domainEntryIter = getDomainEntryIterator();

		while (domainEntryIter.hasNext()) {

			Entry<String, List<FacetFacetValueList>> domainEntry = domainEntryIter
					.next();

			out += FacetEnvironment.DIVIDER;
			out += FacetEnvironment.NEW_LINE;

			out += "domain: " + domainEntry.getKey();
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
