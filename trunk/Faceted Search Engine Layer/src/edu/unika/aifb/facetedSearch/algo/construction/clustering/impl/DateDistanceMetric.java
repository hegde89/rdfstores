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
import java.math.RoundingMode;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;
import edu.unika.aifb.facetedSearch.util.FacetUtil;

/**
 * @author andi
 * 
 */
public class DateDistanceMetric implements IDistanceMetric {

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

	public double getDistance(ILiteral lit1, ILiteral lit2) {

		double distance = Double.NaN;

		try {

			XMLGregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(FacetUtil
					.getLiteralValue(lit1.getValue()));

			XMLGregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(FacetUtil
					.getLiteralValue(lit1.getValue()));

			distance = 0;
			distance += Math.abs(cal2.getYear() - cal1.getYear()) * 365;
			distance += Math.abs(cal2.getMonth() - cal1.getMonth()) * 30;
			distance += Math.abs(cal2.getDay() - cal1.getDay());

			BigDecimal res = (new BigDecimal(distance)).divide(s_maxDistance, 100, RoundingMode.UP);

			assert (res.max(BigDecimal.ONE).equals(BigDecimal.ONE) && (res
					.min(BigDecimal.ZERO).equals(BigDecimal.ZERO) || res
					.longValue() == BigDecimal.ZERO.longValue()));

			return res.doubleValue();

		} catch (NumberFormatException e) {

			s_log
					.error("'" + lit1 + "' or '" + lit2
							+ "' is not a valid date!");

		}

		return new BigDecimal(distance).doubleValue();
	}
}
