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

import java.util.ArrayList;
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
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.util.Util;

public class FacetUtils {

	/*
	 * 
	 */
	private static Logger s_log = Logger.getLogger(FacetUtils.class);

	/*
	 * 
	 */
	private static final String LIST_DELIM = "//";

	public static int getLiteralDataType(String literalString) {

		if (Util.isDataValue(literalString)) {

			StringTokenizer tokenizer = new StringTokenizer(literalString,
					FacetEnvironment.DefaultValue.LITERAL_DELIM);

			String last_token = null;

			while (tokenizer.hasMoreTokens()) {
				last_token = tokenizer.nextToken();
			}

			if (last_token != null) {
				return range2DataType(last_token);
				
			} else {

				s_log.debug("found no datatype attached to literal.");
				return DataType.STRING;
			}
		} else {

			return DataType.STRING;
		}
	}

	public static int range2DataType(String range) {
		
		try {

			URI datatype = new URIImpl(range);

			if (XMLDatatypeUtil.isCalendarDatatype(datatype)) {

				if (datatype.equals(XMLSchema.DATETIME)) {

					return DataType.DATE_TIME;

				} else if (datatype.equals(XMLSchema.TIME)) {

					return DataType.TIME;

				} else if (datatype.equals(XMLSchema.DATE)) {

					return DataType.DATE;

				} else {

					return DataType.STRING;

				}

			} else if (FacetEnvironment.XMLS.NUMERICAL_TYPES
					.contains(datatype.stringValue())) {

				return DataType.NUMERICAL;

			} else if (FacetEnvironment.XMLS.STRING_TYPES
					.contains(datatype.stringValue())) {

				return DataType.STRING;

			} else {

				return DataType.STRING;

			}
		} catch (IllegalArgumentException e) {

			s_log.error("datatype " + range + " is no valid URI!");
			return DataType.STRING;
		}
	}
	
	public static String getLiteralValue(String lit) {

		return lit.lastIndexOf(FacetEnvironment.DefaultValue.LITERAL_DELIM) == -1
				? lit
				: lit
						.substring(
								0,
								lit
										.lastIndexOf(FacetEnvironment.DefaultValue.LITERAL_DELIM));
	}

	public static String getValueOfLiteral(String lit) {

		StringTokenizer tokenizer = new StringTokenizer(lit,
				FacetEnvironment.DefaultValue.LITERAL_DELIM);

		return tokenizer.hasMoreTokens() ? tokenizer.nextToken() : lit;
	}

	public static boolean isDynamicValueCluster(String nodeLabel) {
		return nodeLabel.startsWith("[") && nodeLabel.endsWith("]");
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

	public static Table<String> mergeJoin(Table<String> left,
			List<String> right, String col)
			throws UnsupportedOperationException {

		if (!left.isSorted() || !left.getSortedColumn().equals(col)) {
			throw new UnsupportedOperationException(
					"merge join with unsorted tables");
		}

		List<String> resultColumns = new ArrayList<String>();
		resultColumns.add(col);

		for (String strg : left.getColumnNames()) {
			if (!strg.equals(col)) {
				resultColumns.add(strg);
			}
		}

		int lc = left.getColumn(col);

		Table<String> result = new Table<String>(resultColumns, left.rowCount()
				+ right.size());

		int l = 0, r = 0;

		while ((l < left.rowCount()) && (r < right.size())) {

			String[] lrow = left.getRow(l);
			String rrow = right.get(r);

			int val = lrow[lc].compareTo(rrow);

			if (val < 0) {
				l++;
			} else if (val > 0) {
				r++;
			} else {

				result.addRow(lrow);

				int i = l + 1;
				while ((i < left.rowCount())
						&& (left.getRow(i)[lc].compareTo(rrow) == 0)) {

					result.addRow(left.getRow(i));
					i++;
				}

				l++;
				r++;
			}
		}

		result.setSortedColumn(lc);
		return result;
	}

	public static List<String> string2List(String str) {

		return Arrays.asList(str.split(LIST_DELIM));
	}
}
