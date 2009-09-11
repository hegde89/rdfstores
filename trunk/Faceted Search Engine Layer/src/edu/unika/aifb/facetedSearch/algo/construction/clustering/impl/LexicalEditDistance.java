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

import edu.unika.aifb.facetedSearch.FacetEnvironment;

/**
 * @author andi
 * 
 */
public class LexicalEditDistance {

	/**
	 * @return Costs, according to ascii order of symbols, for transforming
	 *         string1 in string2. Substitution, insertion if |string1| <
	 *         |string2| and deletion if |string2| < |string1| is allowed.
	 */

	public static double getDistance(String string1, String string2) {

		BigDecimal distance = new BigDecimal(0.0);

		// substitution of chars, weighted by their position
		for (int i = 0; i < Math.min(string1.length(), string2.length()); i++) {

			int c1 = (int) string1.charAt(i);
			int c2 = (int) string2.charAt(i);

			if (c1 != c2) {

				BigDecimal augend = new BigDecimal(94);
				augend = augend.pow(i);
				augend = BigDecimal.ONE.divide(augend, 100, RoundingMode.UP);
				augend = augend.multiply(new BigDecimal((c2 + c1) - 32));

				distance = distance.add(augend);
			}
		}

		// insert missing chars, weighted by their position
		if (string1.length() < string2.length()) {

			for (int i = string1.length(); i < string2.length(); i++) {

				int c2 = (int) string2.charAt(i);

				BigDecimal augend = new BigDecimal(94);
				augend = augend.pow(i);
				augend = BigDecimal.ONE.divide(augend, 100, RoundingMode.UP);
				augend = augend.multiply(new BigDecimal(c2 - 32));

				distance = distance.add(augend);
			}
		}
		// deletion of overlapping chars, weighted by their position
		else if (string1.length() > string2.length()) {

			for (int i = string2.length(); i < string1.length(); i++) {

				int c1 = (int) string1.charAt(i);

				BigDecimal augend = new BigDecimal(94);
				augend = augend.pow(i);
				augend = BigDecimal.ONE.divide(augend, 100, RoundingMode.UP);
				augend = augend.multiply(new BigDecimal(c1 - 32));

				distance = distance.add(augend);
			}
		}

		distance = distance.divide(getMaxDifference(string1, string2), 100,
				RoundingMode.UP);

		// assert (distance.max(BigDecimal.ONE).equals(BigDecimal.ONE) &&
		// (distance
		// .min(BigDecimal.ZERO).equals(BigDecimal.ZERO) || distance
		// .longValue() == BigDecimal.ZERO.longValue()));

		return distance.doubleValue();
	}

	public static double getDistance2Root(String string) {
		return getDistance("", string);
	}

	private static BigDecimal getMaxDifference(String string1, String string2) {

		BigDecimal max_dis = new BigDecimal(0);

		for (int i = 0; i < FacetEnvironment.DefaultValue.MAXLENGTH_STRING; i++) {

			BigDecimal augend = new BigDecimal(94);
			augend = augend.pow(i);
			augend = BigDecimal.ONE.divide(augend, 100, RoundingMode.UP);
			augend = augend.multiply(new BigDecimal(2 * 126 - 32));

			max_dis = max_dis.add(augend);
		}

		// // substitution of chars, weighted by their position
		// for (int i = 0; i < Math.min(string1.length(), string2.length());
		// i++) {
		//
		// int c1 = (int) string1.charAt(i);
		// int c2 = (int) string2.charAt(i);
		//
		// if (c1 != c2) {
		//
		// BigDecimal augend = new BigDecimal(94);
		// augend = augend.pow(Math
		// .max(string1.length(), string2.length())
		// - i);
		// augend = augend.multiply(new BigDecimal(2*126 - 32));
		//
		// max_dis = max_dis.add(augend);
		// }
		// }
		//
		// // insert missing chars, weighted by their position
		// if (string1.length() < string2.length()) {
		//
		// for (int i = string1.length(); i < string2.length(); i++) {
		//
		// BigDecimal augend = new BigDecimal(94);
		// augend = augend.pow(Math
		// .max(string1.length(), string2.length())
		// - i);
		// augend = augend.multiply(new BigDecimal(126 - 32));
		//
		// max_dis = max_dis.add(augend);
		// }
		// }
		// // deletion of overlapping chars, weighted by their position
		// else if (string1.length() > string2.length()) {
		//
		// for (int i = string2.length(); i < string1.length(); i++) {
		//
		// BigDecimal augend = new BigDecimal(94);
		// augend = augend.pow(Math
		// .max(string1.length(), string2.length())
		// - i);
		// augend = augend.multiply(new BigDecimal(126 - 32));
		//
		// max_dis = max_dis.add(augend);
		// }
		// }

		return max_dis;
	}
}
