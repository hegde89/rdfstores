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

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;

public class FacetUtils {

	private static Logger s_log = Logger.getLogger(FacetUtils.class);
	private static final String LIST_DELIM = "//";

	public static DataType getLiteralDataType(String literalString) {

		StringTokenizer tokenizer = new StringTokenizer(literalString,
				FacetEnvironment.DefaultValue.LITERAL_DELIM);

		String last_token = null;

		while (tokenizer.hasMoreTokens()) {
			last_token = tokenizer.nextToken();
		}

		if (last_token != null) {

			try {

				URI datatype = new URIImpl(last_token);

				if (XMLDatatypeUtil.isCalendarDatatype(datatype)) {

					if (datatype.equals(XMLSchema.DATETIME)) {

						return DataType.DATE_TIME;

					} else if (datatype.equals(XMLSchema.TIME)) {

						return DataType.TIME;

					} else if (datatype.equals(XMLSchema.DATE)) {

						return DataType.DATE;

					} else {

						return DataType.UNKNOWN;

					}

				} else if (FacetEnvironment.XMLS.NUMERICAL_TYPES
						.contains(datatype.stringValue())) {

					return DataType.NUMERICAL;

				} else if (FacetEnvironment.XMLS.STRING_TYPES.contains(datatype
						.stringValue())) {

					return DataType.STRING;

				} else {

					return DataType.UNKNOWN;

				}
			} catch (IllegalArgumentException e) {

				s_log.error("datatype " + last_token + " is no valid URI!");
				return null;
			}
		} else {

			s_log.debug("found no datatype attached to literal.");
			return null;
		}
	}

	public static String getLiteralValue(String lit) {

		return lit.lastIndexOf(FacetEnvironment.DefaultValue.LITERAL_DELIM) == -1 ? lit
				: lit
						.substring(
								0,
								lit
										.lastIndexOf(FacetEnvironment.DefaultValue.LITERAL_DELIM));
	}

	public static String getValueOfLiteral(String lit) {

		StringTokenizer tokenizer = new StringTokenizer(lit,
				FacetEnvironment.DefaultValue.LITERAL_DELIM);

		return tokenizer.hasMoreTokens() ? tokenizer.nextToken() : null;
	}

	public static String list2String(List<String> list) {

		String out = "";

		for (int i = 0; i < list.size(); i++) {

			out += list.get(i);

			if (i != list.size() - 1) {
				out += LIST_DELIM;
			}
		}

		return out;
	}

	public static List<String> string2List(String str) {

		return Arrays.asList(str.split(LIST_DELIM));
	}
}
