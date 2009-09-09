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
import java.math.RoundingMode;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.index.algo.distance.IDistanceMetric;

/**
 * @author andi
 * 
 */
public class TimeDistanceMetric implements IDistanceMetric {

	private static final BigDecimal s_maxDistance = new BigDecimal(23 * 3600
			+ 59 * 60 + 59);

	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);
	private static TimeDistanceMetric s_instance;

	public static TimeDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new TimeDistanceMetric()
				: s_instance;
	}

	private TimeDistanceMetric() {

	}

	public double getDistance(ILiteral lit1, ILiteral lit2) {

		double distance = Double.NaN;

		try {

			XMLGregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(lit1
					.getValue());

			XMLGregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(lit2
					.getValue());

			distance = 0;
			distance += Math.abs(cal2.getHour() - cal1.getHour()) * 3600;
			distance += Math.abs(cal2.getMinute() - cal1.getMinute()) * 60;
			distance += Math.abs(cal2.getSecond() - cal1.getSecond());

			BigDecimal res = (new BigDecimal(distance)).divide(s_maxDistance, 100, RoundingMode.UP);

			assert (res.max(BigDecimal.ONE).equals(BigDecimal.ONE) && (res
					.min(BigDecimal.ZERO).equals(BigDecimal.ZERO) || res
					.longValue() == BigDecimal.ZERO.longValue()));

			return res.doubleValue();

		} catch (NumberFormatException e) {

			s_log
					.error("'" + lit1 + "' or '" + lit2
							+ "' is not a valid time!");

		}

		return new BigDecimal(distance).doubleValue();
	}
}
