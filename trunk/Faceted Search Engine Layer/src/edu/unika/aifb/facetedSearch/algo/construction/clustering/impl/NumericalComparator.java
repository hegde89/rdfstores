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

import java.math.BigDecimal;
import java.util.Comparator;

import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class NumericalComparator implements Comparator<String> {

//	private SearchSessionCache m_cache;

	public NumericalComparator(SearchSessionCache cache) {
//		m_cache = cache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(String o1, String o2) {

		double litValue1 = lit2Double(o1);
		double litValue2 = lit2Double(o2);

		// m_cache.addParsedLiteral(o1, litValue1);
		// m_cache.addParsedLiteral(o2, litValue2);

		return (new BigDecimal(litValue1)).compareTo(new BigDecimal(litValue2));
	}

	public double lit2Double(String lit) {
		return XMLDatatypeUtil.parseDouble(FacetUtils.getValueOfLiteral(lit));
	}
}
