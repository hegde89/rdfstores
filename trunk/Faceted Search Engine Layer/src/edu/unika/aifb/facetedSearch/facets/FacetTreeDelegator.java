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

import edu.unika.aifb.facetedSearch.algo.ranking.RankingDelegator;
import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class FacetTreeDelegator {

	private SearchSession m_session;
	private boolean m_rankingEnabled;

	private ArrayList<String> m_FVExtensions;
	private ArrayList<String> m_SourceExtensions;

	private Logger m_logger;

	private HashMap<String, FacetTree<Node, Edge>> m_facetTrees;
	private RankingDelegator m_rankingDelegator;

	/*
	 * singleton
	 */
	private static FacetTreeDelegator s_instance;

	public static FacetTreeDelegator getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new FacetTreeDelegator(session)
				: s_instance;
	}

	private FacetTreeDelegator(SearchSession session) {

		this.m_session = session;
		this.m_rankingDelegator = this.m_session.getRankingDelegator();
		this.m_FVExtensions = new ArrayList<String>();
		this.m_SourceExtensions = new ArrayList<String>();
		this.m_facetTrees = new HashMap<String, FacetTree<Node, Edge>>();
		this.m_logger = Logger.getLogger(FacetTreeDelegator.class);
		this.m_rankingEnabled = this.m_session.rankingEnabled();
	}

	public void addFacetTree(String extension) {

		if (!this.m_facetTrees.containsKey(extension)) {
			this.m_facetTrees.put(extension, new FacetTree<Node, Edge>(
					Edge.class));
		} else {
			this.m_logger
					.error("m_facetExtensionGraphs already contained graph for key '"
							+ extension + "'");
		}
	}

	public void clean() {

		this.m_FVExtensions.clear();
		this.m_SourceExtensions.clear();
		this.m_facetTrees.clear();
	}

	public FacetTree<Node, Edge> getFacetTree(String extension) {

		FacetTree<Node, Edge> graph = null;

		if (!this.m_facetTrees.containsKey(extension)) {
			graph = this.m_facetTrees.get(extension);
		} else {
			this.m_logger
					.error("m_facetExtensionGraphs did not contain graph for key '"
							+ extension + "'");
		}

		return graph;
	}

	public Map<String, List<IFacetValueTuple>> getFacetValueTuples() {

		Map<String, List<IFacetValueTuple>> facet_map = new HashMap<String, List<IFacetValueTuple>>();

		for (String extension : this.m_SourceExtensions) {

			facet_map.put(extension, this.getFacetValueTuples(this.m_facetTrees
					.get(extension).getRoot()));
		}

		return facet_map;
	}

	public List<IFacetValueTuple> getFacetValueTuples(Node selection) {

		List<IFacetValueTuple> facetValueList = new ArrayList<IFacetValueTuple>();

		if (this.m_facetTrees.containsKey(selection.getSourceExtension())) {

			FacetTree<Node, Edge> tree = this.m_facetTrees.get(selection
					.getSourceExtension());

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
			this.m_logger
					.error("m_facetTrees did not contain current tree for key '"
							+ selection.getSourceExtension() + "'");
		}

		if (this.m_rankingEnabled) {
			facetValueList = this.m_rankingDelegator.doSorting(facetValueList);
		}

		return facetValueList;
	}

	/**
	 * @return the m_session
	 */
	public SearchSession getSession() {
		return this.m_session;
	}

	/**
	 * @param m_session
	 *            the m_session to set
	 */
	public void setSession(SearchSession session) {
		this.m_session = session;
	}
}
