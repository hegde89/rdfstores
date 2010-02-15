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
package edu.unika.aifb.facetedSearch.facets.converter.inputQuery2graphQuery;

import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class SparQl2GraphQueryConverter extends AbstractConverter {

	private static SparQl2GraphQueryConverter s_instance;

	public static SparQl2GraphQueryConverter getInstance() {
		return s_instance == null
				? s_instance = new SparQl2GraphQueryConverter()
				: s_instance;
	}

	private SparQl2GraphQueryConverter() {

	}

	public StructuredQuery sparQl2StructuredQuery(String sparQlQueryStrg) {

		StructuredQuery sQuery = new StructuredQuery("");

		try {

			SPARQLParser parser = new SPARQLParser();
			ParsedQuery parsedQuery = parser.parseQuery(sparQlQueryStrg,
					FacetEnvironment.DefaultValue.BASE_URI);

			TupleExpr tupleExpr = parsedQuery.getTupleExpr();
			List<StatementPattern> stmtPatterns = StatementPatternCollector
					.process(tupleExpr);

			for (StatementPattern stmtPattern : stmtPatterns) {

				Var subjectVar = stmtPattern.getSubjectVar();
				Var predicateVar = stmtPattern.getPredicateVar();
				Var objectVar = stmtPattern.getObjectVar();

				sQuery.addEdge(subjectVar.getValue() == null
						? FacetEnvironment.DefaultValue.VAR_PREFIX
								+ subjectVar.getName()
						: subjectVar.getValue().stringValue(), predicateVar
						.getValue() == null
						? FacetEnvironment.DefaultValue.VAR_PREFIX
								+ predicateVar.getName()
						: predicateVar.getValue().stringValue(), objectVar
						.getValue() == null
						? FacetEnvironment.DefaultValue.VAR_PREFIX
								+ objectVar.getName()
						: objectVar.getValue().stringValue());

			}

		} catch (MalformedQueryException e) {
			e.printStackTrace();
		}

		return sQuery;
	}
}