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
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class RankingDelegator extends Delegator {

	/*
	 * delegate
	 */
	private RankingMetricPool m_metricPool;

	public RankingDelegator(SearchSession session) {
		m_metricPool = new RankingMetricPool(session);
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

		IRankingMetric metric = m_metricPool.getMetric(FacetedSearchLayerConfig
				.getRankingMetric());

		for (Node node : nodes) {

			if (node instanceof StaticClusterNode) {
				metric.computeScore((StaticClusterNode) node);
			}
		}
	}

	public void computeRanking(Node node) {

		IRankingMetric metric = m_metricPool.getMetric(FacetedSearchLayerConfig
				.getRankingMetric());

		if (node instanceof StaticClusterNode) {
			metric.computeScore((StaticClusterNode) node);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.Delegator#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return m_metricPool != null;
	}
}