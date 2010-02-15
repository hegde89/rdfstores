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
package edu.unika.aifb.facetedSearch.search.evaluator;

import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;

/**
 * @author andi
 * 
 */
public class GenericQueryEvaluatorPool {

	private static GenericQueryEvaluatorPool s_instance;

	public static GenericQueryEvaluatorPool getInstance() {
		return s_instance == null
				? s_instance = new GenericQueryEvaluatorPool()
				: s_instance;
	}

	private GenericQueryEvaluator[] m_evals;

	private GenericQueryEvaluatorPool() {
		m_evals = new GenericQueryEvaluator[FacetedSearchLayerConfig
				.getMaxSearchSessions()];
	}

	public GenericQueryEvaluator get(int sessionID) {
		return m_evals[sessionID];
	}

	public void put(int sessionID, GenericQueryEvaluator eval) {
		m_evals[sessionID] = eval;
	}
}