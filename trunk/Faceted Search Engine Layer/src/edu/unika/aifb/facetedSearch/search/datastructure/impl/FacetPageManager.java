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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.algo.ranking.NodeComparator;
import edu.unika.aifb.facetedSearch.algo.ranking.RankingDelegator;
import edu.unika.aifb.facetedSearch.facets.converter.facet2tree.Facet2TreeModelConverter;
import edu.unika.aifb.facetedSearch.facets.converter.tree2facet.Tree2FacetModelConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueList.CleanType;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Converters;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;

/**
 * @author andi
 * 
 */
public class FacetPageManager {

	private static final int FACETPAGE_KEY = 1;

	/*
	 * 
	 */
	private static Logger s_log = Logger.getLogger(FacetPageManager.class);

	/*
	 * 
	 */
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
	private FacetTreeDelegator m_treeDelegator;
	private RankingDelegator m_rankingDelegator;

	/*
	 * 
	 */
	private Tree2FacetModelConverter m_tree2facetConverter;
	private Facet2TreeModelConverter m_facet2treeConverter;

	/*
	 * stored map
	 */
	private StoredMap<Integer, FacetPage> m_facetPageMap;

	/*
	 * 
	 */
	private EntryBinding<Integer> m_intBinding;
	private EntryBinding<FacetPage> m_fpageBinding;

	/*
	 * 
	 */
	private NodeComparator m_nodeComparator;

	/*
	 * 
	 */
	private boolean m_rankingEnabled;

	private FacetPageManager(SearchSession session) {

		m_session = session;
		m_cache = session.getCache();

		/*
		 * 
		 */
		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);

		m_rankingDelegator = (RankingDelegator) m_session
				.getDelegator(Delegators.RANKING);

		/*
		 * 
		 */
		m_tree2facetConverter = (Tree2FacetModelConverter) m_session
				.getConverter(Converters.TREE2FACET);
		m_facet2treeConverter = (Facet2TreeModelConverter) m_session
				.getConverter(Converters.FACET2TREE);

		/*
		 * 
		 */
		m_nodeComparator = new NodeComparator();

		/*
		 * 
		 */
		m_rankingEnabled = new Boolean(m_session.getProps().getProperty(
				FacetEnvironment.Property.RANKING_ENABLED));

		init();
	}

	public FacetPage getInitialFacetPage() {

		FacetPage fpage = new FacetPage();
		m_treeDelegator.initTrees();

		Iterator<String> domainIter = m_treeDelegator.getDomains().iterator();

		while (domainIter.hasNext()) {

			String domain = domainIter.next();
			Iterator<Node> facetIter = m_treeDelegator.getChildren(domain)
					.iterator();

			while (facetIter.hasNext()) {

				Node facetNode = facetIter.next();

				if ((facetNode.getContent() == NodeContent.DATA_PROPERTY)
						|| (facetNode.getContent() == NodeContent.OBJECT_PROPERTY)
						|| (facetNode.getContent() == NodeContent.TYPE_PROPERTY)) {

					FacetFacetValueList fvList = new FacetFacetValueList();

					fvList
							.setFacet(m_tree2facetConverter
									.node2facet(facetNode));

					List<Node> subFacets = new ArrayList<Node>();
					List<Node> rangeChildren = new ArrayList<Node>();

					Iterator<Node> childrenIter = m_treeDelegator.getChildren(
							domain, facetNode.getID()).iterator();

					while (childrenIter.hasNext()) {

						Node node = childrenIter.next();

						if (node.getContent() == NodeContent.CLASS) {

							rangeChildren.add(node);

						} else if ((node.getContent() == NodeContent.DATA_PROPERTY)
								|| (node.getContent() == NodeContent.OBJECT_PROPERTY)) {

							subFacets.add(node);

						} else {
							s_log.error("should not be here: node '" + node
									+ "'");
						}
					}

					if (m_rankingEnabled) {

						m_rankingDelegator.computeRanking(subFacets);
						Collections.sort(subFacets, m_nodeComparator);

						m_rankingDelegator.computeRanking(rangeChildren);
						Collections.sort(rangeChildren, m_nodeComparator);
					}

					fvList.setFacetValueList(m_tree2facetConverter
							.nodeList2facetValueList(rangeChildren));
					fvList.setSubfacets(m_tree2facetConverter
							.nodeList2facetList(subFacets));

				} else {
					s_log.error("tree structure is not valid! tree: '"
							+ m_treeDelegator.getTree(domain) + "'");
				}
			}
		}

		storeFacetPage(fpage);

		return fpage;
	}

	public FacetPage getRefinedFacetPage(String domain, Facet facet,
			AbstractFacetValue selectedValue) {

		FacetPage fpage = loadPreviousFacetPage();

		if (!selectedValue.isLeave()) {

			Node selectedNode = m_facet2treeConverter
					.facetValue2Node(selectedValue);

			FacetFacetValueList fvList = fpage.getFacetFacetValuesList(domain,
					facet.getUri());

			// browsing in range
			if (selectedNode.getContent() == NodeContent.CLASS) {

				fvList.clean(CleanType.VALUES);

				fvList.addBrowsingObject2History(selectedValue);

				List<Node> nodes = m_treeDelegator.getChildren(domain,
						selectedValue.getNodeId());

				if (m_rankingEnabled) {

					m_rankingDelegator.computeRanking(nodes);
					Collections.sort(nodes, m_nodeComparator);
				}

				fvList.setFacetValueList(m_tree2facetConverter
						.nodeList2facetValueList(nodes));

			}
			// sub-facet
			else if ((selectedNode.getContent() == NodeContent.DATA_PROPERTY)
					|| (selectedNode.getContent() == NodeContent.OBJECT_PROPERTY)
					|| (selectedNode.getContent() == NodeContent.TYPE_PROPERTY)) {

				fvList.clean(CleanType.VALUES);
				fvList.clean(CleanType.SUBFACETS);

				fvList.addBrowsingObject2History(facet);
				fvList.setFacet(m_tree2facetConverter.node2facet(selectedNode));

				Iterator<Node> nodesIter = m_treeDelegator.getChildren(domain,
						selectedValue.getNodeId()).iterator();

				List<Node> subFacets = new ArrayList<Node>();
				List<Node> rangeChildren = new ArrayList<Node>();

				while (nodesIter.hasNext()) {

					Node node = nodesIter.next();

					if (node.getContent() == NodeContent.CLASS) {

						rangeChildren.add(node);

					} else if ((selectedNode.getContent() == NodeContent.DATA_PROPERTY)
							|| (selectedNode.getContent() == NodeContent.OBJECT_PROPERTY)) {

						subFacets.add(node);

					} else {
						s_log.error("should not be here: node '" + selectedNode
								+ "'");
					}
				}

				if (m_rankingEnabled) {

					m_rankingDelegator.computeRanking(subFacets);
					Collections.sort(subFacets, m_nodeComparator);

					m_rankingDelegator.computeRanking(rangeChildren);
					Collections.sort(rangeChildren, m_nodeComparator);
				}

				fvList.setFacetValueList(m_tree2facetConverter
						.nodeList2facetValueList(rangeChildren));
				fvList.setSubfacets(m_tree2facetConverter
						.nodeList2facetList(subFacets));

			} else {
				s_log.error("should not be here: node '" + selectedNode + "'");
			}

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
			m_intBinding = TupleBinding.getPrimitiveBinding(Integer.class);

			m_facetPageMap = new StoredSortedMap<Integer, FacetPage>(m_cache
					.getDB(FacetEnvironment.DatabaseName.FPAGE_CACHE),
					m_intBinding, m_fpageBinding, true);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	private FacetPage loadPreviousFacetPage() {
		return m_facetPageMap.get(FACETPAGE_KEY);
	}

	private boolean storeFacetPage(FacetPage fpage) {
		return m_facetPageMap.put(FACETPAGE_KEY, fpage) == null;
	}
}
