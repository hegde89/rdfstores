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
import java.security.InvalidParameterException;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.ChangePageRequest;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.ExploringKeywordQueryEvaluator;
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
	private Searcher m_hybridQueryEvaluator;

	/*
	 * 
	 */
	private IndexReader m_idxReader;
	private SearchSession m_session;

	/*
	 * 
	 */
	private boolean m_facetsEnabled;

	public GenericQueryEvaluator(SearchSession session, IndexReader idxReader) {

		m_session = session;
		m_idxReader = idxReader;

		m_facetsEnabled = new Boolean(m_session.getProps().getProperty(
				FacetEnvironment.Property.FACETS_ENABLED));
	}

	private void constructResult(Table<String> resultTable, Query query) {

		Result res = new Result();
		res.setResultTable(resultTable);
		res.setQuery(new FacetedQuery((StructuredQuery) query));

		try {

			m_session.getCache().storeCurrentResult(res);

			// create facets for this result set
			if (m_facetsEnabled) {

				FacetPage fpage = m_session.getFacetPageManager()
						.getInitialFacetPage();

				res.setFacetPage(fpage);
				m_session.getCache().storeCurrentResult(res);
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public ResultPage evaluate(Query query) {

		ResultPage resultPage = null;

		if (query instanceof KeywordQuery) {

			// KeywordQueryEvaluator eval = null;
			// Table<String> resultTable;
			//
			// try {
			//
			// m_session.clean(SearchSession.CleanType.ALL);
			//
			// eval = (KeywordQueryEvaluator)
			// getEvaluator(FacetEnvironment.EvaluatorType.KeywordQueryEvaluator);
			// resultTable = eval.evaluate((KeywordQuery) query);
			// constructResult(resultTable, query);
			// resultPage = getFirstResultPage();
			//
			// FacetedQuery fquery = new FacetedQuery(query);
			// m_session.setCurrentQuery(fquery);
			//
			// } catch (IOException e) {
			// e.printStackTrace();
			// } catch (StorageException e) {
			// e.printStackTrace();
			// }

		} else if (query instanceof StructuredQuery) {

			VPEvaluator eval = null;
			Table<String> resultTable;

			try {

				m_session.clean(SearchSession.CleanType.ALL);

				eval = (VPEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);
				resultTable = eval.evaluate((StructuredQuery) query);

				constructResult(resultTable, query);
				resultPage = getFirstResultPage();

				FacetedQuery fquery = new FacetedQuery((StructuredQuery) query);
				m_session.setCurrentQuery(fquery);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (StorageException e) {
				e.printStackTrace();
			}

		} else if (query instanceof HybridQuery) {

			// ExploringHybridQueryEvaluator eval = null;
			// Table<String> resultTable;
			//
			// try {
			//
			// m_session.clean(SearchSession.CleanType.ALL);
			//
			// eval = (ExploringHybridQueryEvaluator)
			// getEvaluator(FacetEnvironment.EvaluatorType.HybridQueryEvaluator);
			// resultTable = eval.evaluate((HybridQuery) query);
			//
			// constructResult(resultTable, query);
			// resultPage = getFirstResultPage();
			//
			// FacetedQuery fquery = new FacetedQuery(query);
			// m_session.setCurrentQuery(fquery);
			//
			// } catch (IOException e) {
			// e.printStackTrace();
			// } catch (StorageException e) {
			// e.printStackTrace();
			// }

		} else if (query instanceof AbstractFacetRequest) {

			FacetRequestEvaluator eval = null;

			try {

				eval = (FacetRequestEvaluator) getEvaluator(FacetEnvironment.EvaluatorType.FacetQueryEvaluator);
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

	protected Searcher getEvaluator(FacetEnvironment.EvaluatorType type)
			throws IOException, StorageException, InvalidParameterException {

		switch (type) {

			case StructuredQueryEvaluator : {

				if (m_vPEvaluator == null) {
					m_vPEvaluator = new VPEvaluator(m_idxReader);
				}

				return m_vPEvaluator;
			}
			case KeywordQueryEvaluator : {

				if (m_keywordQueryEvaluator == null) {
					m_keywordQueryEvaluator = new ExploringKeywordQueryEvaluator(
							m_idxReader);
				}

				return m_keywordQueryEvaluator;
			}
			case FacetQueryEvaluator : {

				if (m_facetQueryEvaluator == null) {
					m_facetQueryEvaluator = new FacetRequestEvaluator(
							m_idxReader, m_session);
				}

				return m_facetQueryEvaluator;
			}
			case ChangePageEvaluator : {

				if (m_changePageEvaluator == null) {
					m_changePageEvaluator = new ChangePageEvaluator(
							m_idxReader, m_session);
				}

				return m_changePageEvaluator;
			}
			case HybridQueryEvaluator : {

				if (m_hybridQueryEvaluator == null) {
					m_hybridQueryEvaluator = new ExploringHybridQueryEvaluator(
							m_idxReader);
				}

				return m_hybridQueryEvaluator;
			}

			default :
				return null;
		}
	}

	private ResultPage getFirstResultPage() {

		ResultPage resPage = ResultPage.EMPTY_PAGE;

		try {
			resPage = m_session.getCache().getCurrentResultPage(1);
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return resPage;
	}
}
