/**
 * 
 * Util.java is based on edu.unika.aifb.graphindex.util.Util.java,
 * edu.unika.aifb.graphindex.searcher.keyword.model.StructureGraphUtil:
 * 
 * -----------------------------------------------------------------------------
 * 
 * Copyright (C) 2009 Günter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * -----------------------------------------------------------------------------
 * 
 * Modifications by Andreas Wagner:
 * 
 * -----------------------------------------------------------------------------
 * 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer project. 
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
 * 
 * -----------------------------------------------------------------------------
 */

package edu.unika.aifb.facetedSearch.util;

import java.util.StringTokenizer;

import edu.unika.aifb.facetedSearch.Environment;
import edu.unika.aifb.facetedSearch.Environment.DataType;
import edu.unika.aifb.facetedSearch.api.model.IAbstractObject;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;

public class Util {

	public static boolean isVariable(String label) {
		return label.startsWith("?");
	}

	public static boolean isConstant(String label) {
		return !isVariable(label);
	}

	public static boolean isDataValue(String label) {
		return !isEntity(label);
	}

	public static boolean isEntity(String label) {

		return label.startsWith("http") || label.startsWith("_:")
				|| label.startsWith("ttp://");
	}

	public static DataType getDataType(ILiteral lit) {

		StringTokenizer tokenizer = new StringTokenizer(((IAbstractObject) lit)
				.getValue(), Environment.DEFAULT_LITERAL_DELIM);
		String last_token = null;

		while (tokenizer.hasMoreTokens()) {
			last_token = tokenizer.nextToken();
		}

		if (last_token != null) {

			if (last_token.toLowerCase().equals(Environment.XML.DataType.DATE)) {
				return DataType.DATE;
			} else if (last_token.toLowerCase().equals(
					Environment.XML.DataType.TIME)) {
				return DataType.TIME;
			} else if (last_token.toLowerCase().equals(
					Environment.XML.DataType.NUMERICAL)) {
				return DataType.NUMERICAL;
			}
			// default value is string
			else {
				return DataType.STRING;
			}
		} else {
			return null;
		}
	}

	public static String getValueOfLiteral(ILiteral lit) {

		StringTokenizer tokenizer = new StringTokenizer(((IAbstractObject) lit)
				.getValue(), Environment.DEFAULT_LITERAL_DELIM);

		return tokenizer.hasMoreTokens() ? tokenizer.nextToken() : null;
	}

	public static String getLocalName(String uri) {

		for (String stopWord : Environment.stopWords) {
			uri = uri.replaceAll(stopWord, "");
		}
		if (uri.lastIndexOf("#") != -1) {
			return uri.substring(uri.lastIndexOf("#") + 1);
		} else if (uri.lastIndexOf("/") != -1) {
			return uri.substring(uri.lastIndexOf("/") + 1);
		} else if (uri.lastIndexOf(":") != -1) {
			return uri.substring(uri.lastIndexOf(":") + 1);
		} else {
			return uri;
		}
	}

}
