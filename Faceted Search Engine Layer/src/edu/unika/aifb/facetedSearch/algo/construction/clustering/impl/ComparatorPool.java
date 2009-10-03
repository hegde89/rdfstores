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
package edu.unika.aifb.facetedSearch.algo.construction.clustering.impl;

import java.util.Comparator;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public class ComparatorPool {

	private static ComparatorPool s_instance;

	public static ComparatorPool getInstance(SearchSessionCache cache) {
		return s_instance == null
				? s_instance = new ComparatorPool(cache)
				: s_instance;
	}

	private StringComparator m_strgComp;
	private NumericalComparator m_numComp;
	private TimeDateComparator m_timeDateComp;

	private ComparatorPool(SearchSessionCache cache) {

		m_strgComp = new StringComparator(cache);
		m_numComp = new NumericalComparator(cache);
		m_timeDateComp = new TimeDateComparator(cache);

	}

	public Comparator<String> getComparator(int dataType) {

		switch (dataType) {
			case DataType.DATE : {
				return m_timeDateComp;
			}
			case DataType.DATE_TIME : {
				return m_timeDateComp;
			}
			case DataType.TIME : {
				return m_timeDateComp;
			}
			case DataType.NUMERICAL : {
				return m_numComp;
			}
			case DataType.STRING : {
				return m_strgComp;
			}
			case DataType.UNKNOWN : {
				return null;
			}
			default :
				return null;
		}
	}
}
