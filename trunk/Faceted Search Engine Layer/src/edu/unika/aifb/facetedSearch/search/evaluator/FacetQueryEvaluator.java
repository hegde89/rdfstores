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
package edu.unika.aifb.facetedSearch.search.evaluator;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.search.datastructure.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.ExpansionRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.RefinementRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;

/**
 * @author andi
 * 
 */
public class FacetQueryEvaluator extends Searcher {

	private static Logger s_log = Logger.getLogger(FacetQueryEvaluator.class);

	private GenericQueryEvaluator m_genericEval;
	private SearchSession m_session;

	public FacetQueryEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);

		m_session = session;
		m_genericEval = session.getStore().getEvaluator();

	}

	public ResultPage evaluate(AbstractFacetRequest facetQuery) {

		if (facetQuery instanceof ExpansionRequest) {

			StructuredQuery structuredQuery = ((ExpansionRequest) facetQuery)
					.getQuery();
			return m_genericEval.evaluate(structuredQuery);

		} else if (facetQuery instanceof RefinementRequest) {

			return null;

		} else {
			s_log.error("facetQuery '" + facetQuery + "'not valid!");
			return null;
		}
	}
}
