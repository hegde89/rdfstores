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
package edu.unika.aifb.facetedSearch.algo.ranking;

import java.util.Collection;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.impl.RankingMetricPool;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class RankingDelegator extends Delegator {

	/*
	 * 
	 */
	private static RankingDelegator s_instance;

	public static RankingDelegator getInstance(SearchSession session) {
		return s_instance == null
				? s_instance = new RankingDelegator(session)
				: s_instance;
	}

	/*
	 * delegate
	 */
	private IRankingMetric m_metric;

	/*
	 * 
	 */
	private RankingMetricPool m_metricPool;

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private SearchSession m_session;

	private RankingDelegator(SearchSession session) {
		m_session = session;
		m_metricPool = RankingMetricPool.getInstance();
	}

	@Override
	public void clean() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.Delegator#close()
	 */
	@Override
	public void close() {

	}

	public void computeRanking(Collection<Node> nodes) {

		m_metric = m_metricPool
				.getMetric(FacetEnvironment.DefaultValue.RANKING_METRIC);

		for (Node node : nodes) {

			if (node instanceof StaticNode) {
				m_metric.computeScore((StaticNode) node);
			}
		}
	}

	public void computeRanking(Node node) {

		m_metric = m_metricPool
				.getMetric(FacetEnvironment.DefaultValue.RANKING_METRIC);

		if (node instanceof StaticNode) {
			m_metric.computeScore((StaticNode) node);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.Delegator#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;
	}
}
