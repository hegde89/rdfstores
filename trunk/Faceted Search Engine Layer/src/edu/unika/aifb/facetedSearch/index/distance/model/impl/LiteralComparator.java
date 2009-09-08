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
package edu.unika.aifb.facetedSearch.index.distance.model.impl;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.index.algo.distance.impl.LexicalEditDistance;

/**
 * @author andi
 * 
 */
public class LiteralComparator implements Comparator<ILiteral> {

	private static Logger s_log = Logger.getLogger(LiteralComparator.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	public int compare(ILiteral lit1, ILiteral lit2) {

		DataType type1 = lit1.getDataType();
		DataType type2 = lit2.getDataType();

		if (type1 == type2) {

			switch (type1) {

			case STRING: {

				BigDecimal dis1 = LexicalEditDistance.getDistance2Root(lit1
						.getValue());
				BigDecimal dis2 = LexicalEditDistance.getDistance2Root(lit2
						.getValue());

				return dis1.compareTo(dis2);
			}
			case NUMERICAL: {

				BigDecimal double1 = new BigDecimal(XMLDatatypeUtil
						.parseDouble(lit1.getValue()));
				BigDecimal double2 = new BigDecimal(XMLDatatypeUtil
						.parseDouble(lit2.getValue()));

				return double1.compareTo(double2);
			}
			case TIME: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						lit1.getValue()).toGregorianCalendar();
				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						lit2.getValue()).toGregorianCalendar();

				return cal1.compareTo(cal2);
			}
			case DATE: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						lit1.getValue()).toGregorianCalendar();

				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						lit2.getValue()).toGregorianCalendar();

				return cal1.compareTo(cal2);
			}
			case DATE_TIME: {

				GregorianCalendar cal1 = XMLDatatypeUtil.parseCalendar(
						lit1.getValue()).toGregorianCalendar();

				GregorianCalendar cal2 = XMLDatatypeUtil.parseCalendar(
						lit2.getValue()).toGregorianCalendar();

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
