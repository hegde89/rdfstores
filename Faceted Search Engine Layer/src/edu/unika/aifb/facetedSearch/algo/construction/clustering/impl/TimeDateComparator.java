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

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class TimeDateComparator implements Comparator<String> {

	private SearchSessionCache m_cache;

	public TimeDateComparator(SearchSessionCache cache) {
		m_cache = cache;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(String o1, String o2) {

		XMLGregorianCalendar cal1 = lit2Cal(o1);
		XMLGregorianCalendar cal2 = lit2Cal(o2);

		m_cache.addParsedLiteral(o1, cal1);
		m_cache.addParsedLiteral(o2, cal2);

		return cal1.toGregorianCalendar().compareTo(cal2.toGregorianCalendar());
	}

	public XMLGregorianCalendar lit2Cal(String lit) {
		return XMLDatatypeUtil.parseCalendar(FacetUtils.getValueOfLiteral(lit));
	}

}
