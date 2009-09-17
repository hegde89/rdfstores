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
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

/**
 * @author andi
 * 
 */
public class LiteralComparator implements Comparator<String> {

	private static Logger s_log = Logger.getLogger(LiteralComparator.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(String lit1, String lit2) {

		DataType type1 = FacetUtils.getLiteralDataType(lit1);
		DataType type2 = FacetUtils.getLiteralDataType(lit2);

		if (type1 == type2) {

			switch (type1) {

			case STRING: {

				double dis1 = LexicalEditDistance.getDistance2Root(FacetUtils
						.getValueOfLiteral(lit1));
				double dis2 = LexicalEditDistance.getDistance2Root(FacetUtils
						.getValueOfLiteral(lit2));

				if (dis1 > dis2) {
					return 1;
				} else if (dis1 < dis2) {
					return -1;
				} else {
					return 0;
				}
			}
			case NUMERICAL: {

				BigDecimal double1 = new BigDecimal(XMLDatatypeUtil
						.parseDouble(FacetUtils.getValueOfLiteral(lit1)));
				BigDecimal double2 = new BigDecimal(XMLDatatypeUtil
						.parseDouble(FacetUtils.getValueOfLiteral(lit2)));

				return double1.compareTo(double2);
			}
			case TIME: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit1))
						.toGregorianCalendar();
				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit2))
						.toGregorianCalendar();

				return cal1.compareTo(cal2);
			}
			case DATE: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit1))
						.toGregorianCalendar();

				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit2))
						.toGregorianCalendar();

				return cal1.compareTo(cal2);
			}
			case DATE_TIME: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit1))
						.toGregorianCalendar();

				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						FacetUtils.getValueOfLiteral(lit2))
						.toGregorianCalendar();

				return cal1.compareTo(cal2);
			}
			default: {

				s_log.error("type1 '" + type1 + "' and type2 '" + type2
						+ "' are unknown!");

				return Integer.MIN_VALUE;
			}
			}

		} else {

			s_log.error("type1 '" + type1 + "' does not equal type2 '" + type2
					+ "'");

			return Integer.MIN_VALUE;
		}
	}
}
