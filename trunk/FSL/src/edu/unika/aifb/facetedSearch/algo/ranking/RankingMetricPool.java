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

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.impl.BrowseAbilityMetric;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.impl.CountFVMetric;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.impl.CountSMetric;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.impl.MinimumEffortMetric;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class RankingMetricPool {

	/*
	 *
	 */
	private static Logger s_log = Logger.getLogger(RankingMetricPool.class);

	/*
	 * 
	 */
	private IRankingMetric m_countS;
	private IRankingMetric m_countFV;
	private IRankingMetric m_browseAbility;
	private IRankingMetric m_minEffort;

	/*
	 * 
	 */
	private SearchSession m_session;

	public RankingMetricPool(SearchSession session) {
		m_session = session;
		init();
	}

	public IRankingMetric getMetric(String type) {

		if (type.equals(FacetedSearchLayerConfig.Value.Ranking.COUNT_S)) {

			return m_countS;

		} else if (type.equals(FacetedSearchLayerConfig.Value.Ranking.COUNT_FV)) {

			return m_countFV;

		} else if (type
				.equals(FacetedSearchLayerConfig.Value.Ranking.BROWSE_ABILITY)) {

			return m_browseAbility;

		} else if (type
				.equals(FacetedSearchLayerConfig.Value.Ranking.MIN_EFFORT)) {

			return m_minEffort;

		} else {

			s_log.error("metric for type '" + type + "' unknown!");
			return null;
		}
	}

	private void init() {

		m_countS = new CountSMetric(m_session);
		m_countFV = new CountFVMetric(m_session);
		m_browseAbility = new BrowseAbilityMetric(m_session);
		m_minEffort = new MinimumEffortMetric(m_session);
	}
}