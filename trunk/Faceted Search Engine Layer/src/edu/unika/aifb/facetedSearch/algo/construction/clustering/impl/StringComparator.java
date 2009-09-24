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

import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class StringComparator implements Comparator<String> {

	// private SearchSessionCache m_cache;

	public StringComparator(SearchSessionCache cache) {
		// m_cache = cache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(String lit1, String lit2) {

		String litValue1 = FacetUtils.getValueOfLiteral(lit1);
		String litValue2 = FacetUtils.getValueOfLiteral(lit2);

		// m_cache.addParsedLiteral(lit1, litValue1);
		// m_cache.addParsedLiteral(lit2, litValue2);

		// return (LexicalEditDistance.getDistance2Root(litValue1))
		// .compareTo(LexicalEditDistance.getDistance2Root(litValue2));

		return litValue1.compareTo(litValue2);
	}
}
