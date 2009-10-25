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

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class Keyword2StructuredQueryConverter {

	private static Keyword2StructuredQueryConverter s_instance;

	public static Keyword2StructuredQueryConverter getInstance() {
		return s_instance == null
				? s_instance = new Keyword2StructuredQueryConverter()
				: s_instance;
	}

	private Keyword2StructuredQueryConverter() {

	}

	public StructuredQuery keyword2StructuredQuery(String keywordQueryStrg) {

		StructuredQuery sQuery = new StructuredQuery("");
		sQuery.addEdge(FacetEnvironment.DefaultValue.VAR,
				FacetEnvironment.DefaultValue.CONTAINS_KEYWORD_PREDICATE,
				keywordQueryStrg);		
		sQuery.setAsSelect(FacetEnvironment.DefaultValue.VAR);

		return sQuery;
	}
}
