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

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;

/**
 * @author andi
 * 
 */
public class NumericalComparator
		implements
			Comparator<AbstractSingleFacetValue> {

	public NumericalComparator() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(AbstractSingleFacetValue o1, AbstractSingleFacetValue o2) {

		Double litValue1 = (Double) ((Literal) o1).getParsedLiteral();
		Double litValue2 = (Double) ((Literal) o2).getParsedLiteral();

		return litValue1.compareTo(litValue2);
	}

	// public double lit2Double(String lit) {
	// return XMLDatatypeUtil.parseDouble(FacetUtils.getValueOfLiteral(lit));
	// }
}
