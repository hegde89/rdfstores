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
import edu.unika.aifb.facetedSearch.search.datastructure.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.ChangePageRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.ClearType;
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

	/*
	 * Evaluators
	 */
	private Searcher m_vPEvaluator;
	private Searcher m_keywordQueryEvaluator;
	private Searcher m_facetQueryEvaluator;
	private Searcher m_changePageEvaluator;

	private IndexReader m_idxReader;
	private SearchSession m_session;

	public GenericQueryEvaluator(SearchSession session, IndexReader idxReader) {
		m_session = session;
		m_idxReader = idxReader;
	}

	private ResultPage constructInitialResultPage(Table<String> results) {

		Table<String> res4Page;
		ResultPage resPage = ResultPage.EMPTY_PAGE;
		m_session.setCurrentPage(1);

		try {

			m_session.getCache().clear(ClearType.ALL);
			m_session.getCache().addResultSet(results);

			if ((res4Page = m_session.getCache().getResults4Page(m_session.getCurrentPage())) != null) {
				resPage = new ResultPage(res4Page, m_session.getCurrentPage());
			}

			// create facets for this result set
			if (new Boolean(m_session.getProps().getProperty(
					FacetEnvironment.FACETS_ENABLED))) {

				((ConstructionDelegator) m_session
						.getDelegator(Delegators.CONSTRUCTION))
						.doFacetConstruction(results);

				// resPage.setFacets();
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
				resultPage = constructInitialResultPage(resultTable);

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
				resultPage = constructInitialResultPage(resultTable);

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
				resultPage = constructInitialResultPage(resultTable);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}

		} else if (query instanceof AbstractFacetRequest) {

			FacetQueryEvaluator eval = null;

			try {

				eval = (FacetQueryEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.FacetQueryEvaluator);
				resultPage = eval.evaluate((AbstractFacetRequest) query);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}
		} else if (query instanceof ChangePageRequest) {

			ChangePageEvaluator eval = null;

			try {

				eval = (ChangePageEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.ChangePageEvaluator);
				resultPage = eval.evaluate((ChangePageRequest) query);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}
		}

		return resultPage;
	}

	private Searcher getEvaluator(FacetEnvironment.EvaluatorType type)
			throws IOException, StorageException, InvalidParameterException {

		switch (type) {

			case StructuredQueryEvaluator : {

				if (m_vPEvaluator == null) {
					m_vPEvaluator = new VPEvaluator(m_idxReader);
				}

				return m_vPEvaluator;
			}
				// case CombinedQueryEvaluator: {
				// searcher = new CombinedQueryEvaluator(this.m_idxReader);
				// break;
				// }
			case KeywordQueryEvaluator : {

				if (m_keywordQueryEvaluator == null) {
					m_keywordQueryEvaluator = new ExploringKeywordQueryEvaluator(
							m_idxReader);
				}

				return m_keywordQueryEvaluator;
			}
			case FacetQueryEvaluator : {

				if (m_facetQueryEvaluator == null) {
					m_facetQueryEvaluator = new FacetQueryEvaluator(m_idxReader, m_session);
				}

				return m_facetQueryEvaluator;
			}
			case ChangePageEvaluator : {

				if (m_changePageEvaluator == null) {
					m_changePageEvaluator = new ChangePageEvaluator(m_idxReader, m_session);
				}

				return m_changePageEvaluator;
			}
			default : {
				throw new InvalidParameterException(ExceptionHelper
						.createMessage("EvaluatorType",
								ExceptionHelper.Cause.NOT_VALID));
			}
		}
	}
}
