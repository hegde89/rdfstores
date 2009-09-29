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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.converter.tree2facet.Tree2FacetModelConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Converters;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;

/**
 * @author andi
 * 
 */
public class FacetPageManager {

	private static FacetPageManager s_instance;

	public static FacetPageManager getInstance(SearchSession session) {
		return s_instance == null
				? s_instance = new FacetPageManager(session)
				: s_instance;
	}

	/*
	 * 
	 */
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private Tree2FacetModelConverter m_tree2facetConverter;
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * stored map
	 */
	private StoredMap<String, FacetPage> m_facetPageMap;

	/*
	 * 
	 */
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<FacetPage> m_fpageBinding;

	private FacetPageManager(SearchSession session) {

		m_session = session;
		m_cache = session.getCache();

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
		m_tree2facetConverter = (Tree2FacetModelConverter) m_session
				.getConverter(Converters.TREE2FACET);

		init();
	}

	public FacetPage getInitialFacetPage() {

		FacetPage fpage;

		if ((fpage = loadPreviousFacetPage()) == null) {
			// TODO
		}

		return fpage;
	}

	public FacetPage getRefinedFacetPage(String domain, Facet facet,
			AbstractFacetValue selectedValue) {

		FacetPage fpage = loadPreviousFacetPage();

		if (!selectedValue.isLeave()) {

			FacetFacetValueList fvList = fpage.getFacetFacetValuesList(domain,
					facet.getUri());
			fvList.addFacetValue2History(selectedValue);
			fvList.setFacetValueList(m_tree2facetConverter
					.nodeList2facetValueList(m_treeDelegator.getChildren(
							domain, selectedValue.getNodeId())));

			storeFacetPage(fpage);
		}

		return fpage;
	}

	private void init() {

		StoredClassCatalog cata;

		try {

			cata = new StoredClassCatalog(m_cache
					.getDB(FacetEnvironment.DatabaseName.CLASS));

			m_fpageBinding = new SerialBinding<FacetPage>(cata, FacetPage.class);
			m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

			m_facetPageMap = new StoredSortedMap<String, FacetPage>(m_cache
					.getDB(FacetEnvironment.DatabaseName.FPAGE_CACHE),
					m_strgBinding, m_fpageBinding, true);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	private FacetPage loadPreviousFacetPage() {

		// TODO

		return null;
	}

	private boolean storeFacetPage(FacetPage fpage) {

		// TODO

		return false;
	}
}
