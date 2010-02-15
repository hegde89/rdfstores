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

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;

/**
 * @author andi
 * 
 */
public class TimeDateComparator implements Comparator<AbstractSingleFacetValue> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(AbstractSingleFacetValue o1, AbstractSingleFacetValue o2) {

		XMLGregorianCalendar cal1 = (XMLGregorianCalendar) ((Literal) o1)
				.getParsedLiteral();
		XMLGregorianCalendar cal2 = (XMLGregorianCalendar) ((Literal) o2)
				.getParsedLiteral();

		return cal1.toGregorianCalendar().compareTo(cal2.toGregorianCalendar());
	}

	// public XMLGregorianCalendar lit2Cal(String lit) {
	// return XMLDatatypeUtil.parseCalendar(FacetUtils.getValueOfLiteral(lit));
	// }
}
