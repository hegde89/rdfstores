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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.ExpansionRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.RefinementRequest;
import edu.unika.aifb.facetedSearch.search.history.QueryHistoryManager;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.Searcher;

/**
 * @author andi
 * 
 */
public class FacetRequestEvaluator extends Searcher {

	private static Logger s_log = Logger.getLogger(FacetRequestEvaluator.class);

	private QueryHistoryManager m_history;
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	public FacetRequestEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);

		m_session = session;
		m_history = session.getHistory();
		m_cache = session.getCache();

	}

	public ResultPage evaluate(AbstractFacetRequest facetRequest) {

		m_session.setCurrentPage(1);

		if (facetRequest instanceof ExpansionRequest) {

			ResultPage resPage = ResultPage.EMPTY_PAGE;
			ExpansionRequest expReq = (ExpansionRequest) facetRequest;

			/*
			 * update query
			 */
			FacetedQuery fquery = m_session.getCurrentQuery();
			fquery.removeAllFacetFacetValueTuples(expReq.getTuples());
			m_session.setCurrentQuery(fquery);

			if (m_history.containsResult(fquery.getId())) {

				try {

					m_cache.storeResult(m_history.getResult(fquery.getId()));

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				}

				try {

					resPage = m_cache.getResultPage(m_session.getCurrentPage());

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				// TODO
			}
			
			return resPage;

		} else if (facetRequest instanceof RefinementRequest) {

			RefinementRequest refReq = (RefinementRequest) facetRequest;

			/*
			 * update query
			 */
			FacetedQuery fquery = m_session.getCurrentQuery();
			fquery.addFacetFacetValueTuple(refReq.getTuple());

			m_session.setCurrentQuery(fquery);

			return null;

		} else {
			s_log.error("facetRequest '" + facetRequest + "'not valid!");
			return ResultPage.EMPTY_PAGE;
		}
	}
}
