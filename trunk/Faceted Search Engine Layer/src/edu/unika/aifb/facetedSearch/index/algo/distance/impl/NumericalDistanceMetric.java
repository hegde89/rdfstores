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
package edu.unika.aifb.facetedSearch.index.algo.distance.impl;

import java.math.BigDecimal;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.index.algo.distance.IDistanceMetric;

/**
 * @author andi
 * 
 */
public class NumericalDistanceMetric implements IDistanceMetric {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);
	private static NumericalDistanceMetric s_instance;

	public static NumericalDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new NumericalDistanceMetric()
				: s_instance;
	}

	private NumericalDistanceMetric() {
	}

	public BigDecimal getDistance(ILiteral lit1, ILiteral lit2) {

		double double1 = XMLDatatypeUtil.parseDouble(lit1.getValue());
		double double2 = XMLDatatypeUtil.parseDouble(lit2.getValue());

		return new BigDecimal(Math.abs(double1 - double2));
	}
}
