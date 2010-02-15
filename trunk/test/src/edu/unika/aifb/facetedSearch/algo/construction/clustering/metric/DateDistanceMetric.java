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
import java.math.RoundingMode;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;

/**
 * @author andi
 * 
 */
public class DateDistanceMetric implements
		IDistanceMetric<XMLGregorianCalendar> {

	private static final BigDecimal s_maxDistance = new BigDecimal(2000 * 365
			+ 11 * 30 + 29);

	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);
	private static DateDistanceMetric s_instance;

	public static DateDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new DateDistanceMetric()
				: s_instance;
	}

	private DateDistanceMetric() {

	}

	public BigDecimal getDistance(XMLGregorianCalendar lit1,
			XMLGregorianCalendar lit2) {

		double distance = Double.NaN;

		try {

			distance = 0;
			distance += Math.abs(lit2.getYear() - lit1.getYear()) * 365;
			distance += Math.abs(lit2.getMonth() - lit1.getMonth()) * 30;
			distance += Math.abs(lit2.getDay() - lit1.getDay());

			BigDecimal res = (new BigDecimal(distance)).divide(s_maxDistance,
					100, RoundingMode.UP);

			// assert (res.max(BigDecimal.ONE).equals(BigDecimal.ONE) &&
			// (res.min(
			// BigDecimal.ZERO).equals(BigDecimal.ZERO) || (res
			// .longValue() == BigDecimal.ZERO.longValue())));

			return res;

		} catch (NumberFormatException e) {

			s_log
					.error("'" + lit1 + "' or '" + lit2
							+ "' is not a valid date!");

		}

		return new BigDecimal(distance);
	}
}
