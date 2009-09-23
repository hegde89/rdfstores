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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public class FacetTreeDelegator extends Delegator {

	private static Logger s_logger = Logger.getLogger(FacetTreeDelegator.class);

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	private ArrayList<HashMap<? extends Object, ? extends Object>> m_maps;

	private HashMap<String, FacetTree> m_domain2treeMap;
	private HashMap<Double, FacetTree> m_node2treeMap;
	private HashMap<String, Double> m_domain2currentNode;

	private static FacetTreeDelegator s_instance;

	public static FacetTreeDelegator getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new FacetTreeDelegator(session)
				: s_instance;
	}

	private FacetTreeDelegator(SearchSession session) {

		m_session = session;
		m_cache = m_session.getCache();

		init();
		
	}

	public void addSubTree4Node(Double nodeId, FacetTree tree) {
		m_node2treeMap.put(nodeId, tree);
	}

	public void addTree4Domain(String domain, FacetTree tree) {
		m_domain2treeMap.put(domain, tree);
	}

	public void clear() {

		for (HashMap<? extends Object, ? extends Object> map : m_maps) {
			map.clear();
		}
	}

	public FacetTree getFacetTree(String domain) {

		return m_domain2treeMap.get(domain);
	}

	public Map<String, List<IFacetValueTuple>> getFacetValueTuples() {

		Map<String, List<IFacetValueTuple>> facet_map = new HashMap<String, List<IFacetValueTuple>>();

		for (String extension : this.m_SourceExtensions) {

			facet_map.put(extension, this
					.getFacetValueTuples(this.m_domain2treeMap.get(extension)
							.getRoot()));
		}

		return facet_map;
	}

	public List<IFacetValueTuple> getFacetValueTuples(Node selection) {

		List<IFacetValueTuple> facetValueList = new ArrayList<IFacetValueTuple>();

		if (this.m_domain2treeMap.containsKey(selection.getDomain())) {

			FacetTree tree = this.m_domain2treeMap.get(selection.getDomain());

			Iterator<Edge> iter = tree.outgoingEdgesOf(selection).iterator();

			Edge current_facet;
			Node current_value;

			while (iter.hasNext()) {

				current_value = tree.getEdgeTarget(current_facet = iter.next());

				if (this.m_rankingEnabled) {
					this.m_rankingDelegator.doRanking(current_facet,
							current_value);
				}

				facetValueList.add(new FacetValueTuple(current_facet,
						current_value));
			}
		} else {
			FacetTreeDelegator.s_logger
					.error("m_facetTrees did not contain current tree for key '"
							+ selection.getDomain() + "'");
		}

		if (this.m_rankingEnabled) {
			facetValueList = this.m_rankingDelegator.doSorting(facetValueList);
		}

		return facetValueList;
	}

	private void init() {
		// init stuff
		m_node2treeMap = new HashMap<Double, FacetTree>();
		m_domain2treeMap = new HashMap<String, FacetTree>();
		m_domain2currentNode = new HashMap<String, Double>();

		m_maps = new ArrayList<HashMap<? extends Object, ? extends Object>>();
		m_maps.add(m_domain2treeMap);
		m_maps.add(m_node2treeMap);
		m_maps.add(m_domain2currentNode);
	}
}
