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

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
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

	public static String cleanURI(String dirtyURI) {

		String cleanURI = new String();

		if (dirtyURI.startsWith("_:")) {

			cleanURI = "http:" + dirtyURI.substring(2);
			return cleanURI;

		} else if (dirtyURI.startsWith("ttp:")) {
			cleanURI = "http:" + dirtyURI.substring(4);
			return cleanURI;
		} else {
			return dirtyURI;
		}
	}

	public static String encodeLocalName(String url) {

		try {

			int endIdx = url.indexOf("#") > 0 ? url.indexOf("#") : url
					.lastIndexOf("/");

			String prefix = url.substring(0, endIdx + 1);
			String localname = URLEncoder.encode(url.substring(endIdx + 1),
					FacetEnvironment.UTF8);

			return prefix + localname;

		} catch (Exception e) {

			e.printStackTrace();
			return url;
		}
	}

	public static List<String> getColumnsNames4Table(StructuredQuery sq) {

		List<String> columns = new ArrayList<String>();
		QNode startNode = sq.getSelectVariables().get(0);
		columns.add(startNode.getLabel());

		QueryGraph qGraph = sq.getQueryGraph();

		Stack<QueryEdge> outEdgesStack = new Stack<QueryEdge>();
		outEdgesStack.addAll(qGraph.outgoingEdgesOf(startNode));

		while (!outEdgesStack.isEmpty()) {

			QueryEdge edge = outEdgesStack.pop();
			QNode tar = edge.getTarget();

			if (!edge.getProperty().equals(
					FacetEnvironment.RDF.NAMESPACE + FacetEnvironment.RDF.TYPE)) {

				if (FacetUtils.isVariable(tar.getLabel())) {
					columns.add(tar.getLabel());
				} else {
					columns.add(edge.getSource().getLabel() + " - "
							+ Util.truncateUri(edge.getProperty()) + " - "
							+ Util.truncateUri(tar.getLabel()));
				}

				outEdgesStack.addAll(qGraph.outgoingEdgesOf(tar));
			}
		}

		return columns;
	}

	public static int getLiteralDataType(String literalString) {

		if (Util.isDataValue(literalString)) {

			if (literalString
					.contains(FacetEnvironment.DefaultValue.LITERAL_DELIM)) {

				StringTokenizer tokenizer = new StringTokenizer(literalString,
						FacetEnvironment.DefaultValue.LITERAL_DELIM);

				String last_token = null;

				while (tokenizer.hasMoreTokens()) {
					last_token = tokenizer.nextToken();
				}

				if (last_token != null) {
					return range2DataType(last_token);
				} else {
					return DataType.STRING;
				}
			} else {

				s_log.debug("found no datatype attached to literal.");
				return DataType.STRING;
			}
		} else {

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

	public static String getName4DynamicNode(DynamicNode dyn) {

		String name;
		int diffPos = 0;

		while ((dyn.getLeftBorder().length() < diffPos)
				&& (dyn.getRightBorder().length() < diffPos)
				&& (dyn.getLeftBorder().charAt(diffPos) == dyn.getRightBorder()
						.charAt(diffPos))) {
			diffPos++;
		}

		name = "[" + dyn.getLeftBorder().substring(0, diffPos + 1)
				+ (diffPos < dyn.getLeftBorder().length() ? "..." : "") + " - "
				+ dyn.getRightBorder().substring(0, diffPos + 1)
				+ (diffPos < dyn.getRightBorder().length() ? "..." : "") + "]";

		return name;
	}

	public static String getNiceName(String dirtyString) {

//		dirtyString = dirtyString.replaceAll("\\/|:|\\.|#|\\?|&|\\+|-|~", "_");

		if (dirtyString.length() > FacetEnvironment.DefaultValue.MAXLENGTH_STRING) {
			dirtyString = dirtyString.substring(0,
					FacetEnvironment.DefaultValue.MAXLENGTH_STRING - 1) + "...";
		}

		return dirtyString;
	}

	public static String getValueOfLiteral(String lit) {

		StringTokenizer tokenizer = new StringTokenizer(lit,
				FacetEnvironment.DefaultValue.LITERAL_DELIM);

		return tokenizer.hasMoreTokens() ? tokenizer.nextToken() : lit;
	}

	public static boolean hasDataTypeAttached(String lit) {
		return lit.contains(FacetEnvironment.DefaultValue.LITERAL_DELIM);
	}

	public static boolean isDirty(String uri) {
		return uri.startsWith("_:") || uri.startsWith("ttp://");
	}

	public static boolean isGenericNode(Node node) {

		if (node.getValue().startsWith("[") && node.getValue().endsWith("]")) {
			return true;
		} else {
			return node.isGeneric();
		}
	}

	public static boolean isVariable(String columnLabel) {
		return Util.isVariable(columnLabel) && !columnLabel.contains(" - ");
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

		for (String strg : left.getColumnNames()) {
			resultColumns.add(strg);
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

	// public static String getDataType4Facet(String facet) {
	//
	// String range = null;
	//
	// switch (type) {
	//
	// case FacetEnvironment.DataType.DATE : {
	// range = XMLSchema.DATE.toString();
	// break;
	// }
	// case FacetEnvironment.DataType.DATE_TIME : {
	// range = XMLSchema.DATETIME.stringValue();
	// break;
	// }
	// case FacetEnvironment.DataType.NUMERICAL : {
	// range = "";
	// break;
	// }
	// case FacetEnvironment.DataType.STRING : {
	// range = XMLSchema.STRING.stringValue();
	// break;
	// }
	// case FacetEnvironment.DataType.TIME : {
	// range = XMLSchema.TIME.toString();
	// break;
	// }
	// default : {
	// range = "";
	// break;
	// }
	// }
	//
	// return range;
	// }

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

			} else if (FacetEnvironment.XMLS.NUMERICAL_TYPES.contains(datatype
					.stringValue())) {

				return DataType.NUMERICAL;

			} else if (FacetEnvironment.XMLS.STRING_TYPES.contains(datatype
					.stringValue())) {

				return DataType.STRING;

			} else {

				return DataType.STRING;

			}
		} catch (IllegalArgumentException e) {

			s_log.error("datatype " + range + " is no valid URI!");
			return DataType.STRING;
		}
	}

	public static List<String> string2List(String str) {

		return Arrays.asList(str.split(LIST_DELIM));
	}
}
