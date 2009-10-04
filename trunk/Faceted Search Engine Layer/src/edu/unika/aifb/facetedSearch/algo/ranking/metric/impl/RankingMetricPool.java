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
package edu.unika.aifb.facetedSearch.algo.ranking.metric.impl;

import edu.unika.aifb.facetedSearch.FacetEnvironment.RankingMetricType;
import edu.unika.aifb.facetedSearch.algo.ranking.metric.IRankingMetric;

/**
 * @author andi
 * 
 */
public class RankingMetricPool {

	/*
	 * 
	 */
	private static RankingMetricPool s_instance;

	public static RankingMetricPool getInstance() {
		return s_instance == null
				? s_instance = new RankingMetricPool()
				: s_instance;
	}

	/*
	 * 
	 */
	private IRankingMetric m_countS;
	private IRankingMetric m_countFV;
	private IRankingMetric m_browseAbility;

	private RankingMetricPool() {
		init();
	}

	public IRankingMetric getMetric(int type) {

		switch (type) {

			case RankingMetricType.COUNT_S : {
				return m_countS;
			}
			case RankingMetricType.COUNT_FV : {
				return m_countFV;
			}
			case RankingMetricType.BROWSE_ABILITY : {
				return m_browseAbility;
			}

			default :
				return null;
		}

	}

	private void init() {

		m_countS = new CountSMetric();
		m_countFV = new CountFVMetric();
		m_browseAbility = new BrowseAbilityMetric();
	}
}
