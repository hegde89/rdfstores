/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer project. 
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
package edu.unika.aifb.facetedSearch.facets;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.CleanType;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;

/**
 * @author andi
 * 
 */
public class FacetTreeDelegator extends Delegator {

	private static FacetTreeDelegator s_instance;
	@SuppressWarnings("unused")
	private static Logger s_logger = Logger.getLogger(FacetTreeDelegator.class);

	public static FacetTreeDelegator getInstance(SearchSession session) {
		return s_instance == null
				? s_instance = new FacetTreeDelegator(session)
				: s_instance;
	}

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	/*
	 * stored maps ...
	 */
	private StoredMap<String, FacetTree> m_domain2treeMap;
	private StoredMap<String, FacetTree> m_node2subTreeMap;

	/*
	 * bindings
	 */
	private EntryBinding<FacetTree> m_treeBinding;
	private EntryBinding<String> m_strgBinding;

	/*
	 * other maps
	 */
	private ArrayList<HashMap<? extends Object, ? extends Object>> m_maps;
	private HashMap<String, Double> m_domain2currentRoot;

	private FacetTreeDelegator(SearchSession session) {

		m_session = session;
		m_cache = m_session.getCache();

		try {

			init();

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void addSubTree4Node(Node node, FacetTree tree) {
		m_node2subTreeMap.put(String.valueOf(node.getID()), tree);
	}

	public void addTree4Domain(String domain, FacetTree tree) {
		m_domain2treeMap.put(domain, tree);
	}

	public void clean() {
		close();
		reOpen();
	}

	public void close() {

		for (HashMap<? extends Object, ? extends Object> map : m_maps) {
			map.clear();
		}

		try {

			m_cache.clean(CleanType.TREES);

			m_domain2treeMap = null;
			m_node2subTreeMap = null;

			System.gc();

		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public FacetPage getCurrentFacetPage() {

		// TODO

		return null;
	}

	public FacetTree getFacetTree(String domain) {

		return m_domain2treeMap.get(domain);
	}

	// public List<IFacetFacetValueList> getFacetValueTuples(Node selection) {
	//
	// List<IFacetFacetValueList> facetValueList = new
	// ArrayList<IFacetFacetValueList>();
	//
	// if (this.m_domain2treeMap.containsKey(selection.getDomain())) {
	//
	// FacetTree tree = this.m_domain2treeMap.get(selection.getDomain());
	//
	// Iterator<Edge> iter = tree.outgoingEdgesOf(selection).iterator();
	//
	// Edge current_facet;
	// Node current_value;
	//
	// while (iter.hasNext()) {
	//
	// current_value = tree.getEdgeTarget(current_facet = iter.next());
	//
	// if (m_rankingEnabled) {
	// m_rankingDelegator.doRanking(current_facet, current_value);
	// }
	//
	// facetValueList.add(new FacetFacetValueList(current_facet,
	// current_value));
	// }
	// } else {
	// FacetTreeDelegator.s_logger
	// .error("m_facetTrees did not contain current tree for key '"
	// + selection.getDomain() + "'");
	// }
	//
	// if (m_rankingEnabled) {
	// facetValueList = m_rankingDelegator.doSorting(facetValueList);
	// }
	//
	// return facetValueList;
	// }

	private void init() throws IllegalArgumentException, DatabaseException {

		StoredClassCatalog cata = new StoredClassCatalog(m_cache
				.getDB(FacetDbUtils.DatabaseNames.CLASS));

		m_treeBinding = new SerialBinding<FacetTree>(cata, FacetTree.class);
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

		/*
		 * stored maps
		 */
		m_node2subTreeMap = new StoredSortedMap<String, FacetTree>(m_cache
				.getDB(FacetDbUtils.DatabaseNames.FTREE_CACHE), m_strgBinding,
				m_treeBinding, true);

		m_domain2treeMap = new StoredSortedMap<String, FacetTree>(m_cache
				.getDB(FacetDbUtils.DatabaseNames.FTREE_CACHE), m_strgBinding,
				m_treeBinding, true);

		/*
		 * other maps
		 */
		m_domain2currentRoot = new HashMap<String, Double>();
		m_maps = new ArrayList<HashMap<? extends Object, ? extends Object>>();
		m_maps.add(m_domain2currentRoot);
	}

	private void reOpen() {

		/*
		 * stored maps
		 */
		if (m_node2subTreeMap == null) {
			m_node2subTreeMap = new StoredSortedMap<String, FacetTree>(m_cache
					.getDB(FacetDbUtils.DatabaseNames.FTREE_CACHE),
					m_strgBinding, m_treeBinding, true);
		}
		if (m_domain2treeMap == null) {
			m_domain2treeMap = new StoredSortedMap<String, FacetTree>(m_cache
					.getDB(FacetDbUtils.DatabaseNames.FTREE_CACHE),
					m_strgBinding, m_treeBinding, true);
		}
	}
}
