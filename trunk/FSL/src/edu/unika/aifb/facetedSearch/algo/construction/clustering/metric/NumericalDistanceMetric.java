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
package edu.unika.aifb.facetedSearch.algo.construction.clustering.metric;

import java.math.BigDecimal;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;

/**
 * @author andi
 * 
 */
public class NumericalDistanceMetric implements IDistanceMetric<Double> {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);
	private static NumericalDistanceMetric s_instance;

	public static NumericalDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new NumericalDistanceMetric()
				: s_instance;
	}

	private NumericalDistanceMetric() {
	}

	public BigDecimal getDistance(Double lit1, Double lit2) {
		return new BigDecimal(Math.abs(lit1 - lit2));
	}
}
