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
import java.security.InvalidParameterException;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.exception.ExceptionHelper;
import edu.unika.aifb.facetedSearch.search.datastructure.FacetQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.ExploringKeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 */
public class GenericQueryEvaluator {

	private IndexReader m_idxReader;
	private int m_currentPage;
	private SearchSession m_session;

	public GenericQueryEvaluator(SearchSession session, IndexReader idxReader) {
		m_session = session;
		m_idxReader = idxReader;
	}

	private ResultPage constructFirstResultPage(Table<String> results) {

		Table<String> res4Page;
		ResultPage resPage = ResultPage.EMPTY_PAGE;
		m_currentPage = 1;

		try {

			m_session.getCache().clear();
			m_session.getCache().storeResultSet(results);

			if ((res4Page = m_session.getCache().getResults4Page(m_currentPage)) != null) {
				resPage = new ResultPage(res4Page, m_currentPage);
			}

			// create facets for this result set
			if (Boolean.getBoolean(m_session.getProps().getProperty(
					FacetEnvironment.FACETS_ENABLED))) {
				resPage.setFacets(((ConstructionDelegator) m_session
						.getDelegator(Delegators.CONSTRUCTION))
						.doFacetConstruction(results));
			}

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}

		return resPage;
	}

	public ResultPage evaluate(Query query) {

		ResultPage resultPage = null;

		if (query instanceof KeywordQuery) {

			KeywordQueryEvaluator eval = null;
			Table<String> resultTable;

			try {

				eval = (KeywordQueryEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.KeywordQueryEvaluator);
				resultTable = eval.evaluate((KeywordQuery) query);
				resultPage = constructFirstResultPage(resultTable);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}

		} else if (query instanceof StructuredQuery) {

			VPEvaluator eval = null;
			Table<String> resultTable;

			try {

				eval = (VPEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);
				resultTable = eval.evaluate((StructuredQuery) query);
				resultPage = constructFirstResultPage(resultTable);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}

		} else if (query instanceof HybridQuery) {

			ExploringHybridQueryEvaluator eval = null;
			Table<String> resultTable;

			try {

				eval = (ExploringHybridQueryEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.HybridQueryEvaluator);
				resultTable = eval.evaluate((HybridQuery) query);
				resultPage = constructFirstResultPage(resultTable);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}

		} else if (query instanceof FacetQuery) {

			// TODO

		}

		return resultPage;
	}

	private Searcher getEvaluator(FacetEnvironment.EvaluatorType type)
			throws IOException, StorageException, InvalidParameterException {

		Searcher searcher = null;

		switch (type) {

		case StructuredQueryEvaluator: {
			searcher = new VPEvaluator(this.m_idxReader);
			break;
		}
			// case CombinedQueryEvaluator: {
			// searcher = new CombinedQueryEvaluator(this.m_idxReader);
			// break;
			// }
		case KeywordQueryEvaluator: {
			searcher = new ExploringKeywordQueryEvaluator(this.m_idxReader);
			break;
		}
		case FacetQueryEvaluator: {
			searcher = new FacetQueryEvaluator(this.m_idxReader);
			break;
		}
		default: {
			throw new InvalidParameterException(ExceptionHelper.createMessage(
					"EvaluatorType", ExceptionHelper.Cause.NOT_VALID));
		}
		}

		return searcher;
	}

	public ResultPage getResultPage(int page) {

		Table<String> res4Page;
		ResultPage resPage = ResultPage.EMPTY_PAGE;

		try {

			if ((res4Page = m_session.getCache().getResults4Page(page)) != null) {
				resPage = new ResultPage(res4Page, page);
			}

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return resPage;
	}
}
