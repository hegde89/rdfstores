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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.converter.facet2query.Facet2QueryModelConverter;
import edu.unika.aifb.facetedSearch.facets.converter.facet2tree.Facet2TreeModelConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractRefinementRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.BrowseRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.ExpansionRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.FacetValueRefinementRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.KeywordRefinementRequest;
import edu.unika.aifb.facetedSearch.search.fpage.FacetPageManager;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.CleanType;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Converters;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.TranslatedQuery;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetRequestEvaluator extends Searcher {

	private static Logger s_log = Logger.getLogger(FacetRequestEvaluator.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private FacetPageManager m_fpageManager;

	/*
	 * 
	 */
	private Facet2TreeModelConverter m_facet2TreeModelConverter;
	private Facet2QueryModelConverter m_facet2QueryModelConverter;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * 
	 */
	private FacetRequestHelper m_helper;

	public FacetRequestEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);

		m_session = session;

		m_facet2TreeModelConverter = (Facet2TreeModelConverter) session
				.getConverter(Converters.FACET2TREE);
		m_facet2QueryModelConverter = (Facet2QueryModelConverter) session
				.getConverter(Converters.FACET2QUERY);

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);

		m_helper = new FacetRequestHelper(session);

		m_fpageManager = session.getFacetPageManager();

	}

	public Object evaluate(AbstractFacetRequest facetRequest) {

		if (facetRequest instanceof ExpansionRequest) {

			m_session.setCurrentPage(1);
			m_session.clean(CleanType.ALL);

			ResultPage resPage = ResultPage.EMPTY_PAGE;
			ExpansionRequest expansionReq = (ExpansionRequest) facetRequest;

			/*
			 * update query
			 */
			FacetedQuery fquery = m_session.getCurrentQuery();
			boolean removedPath = fquery.removePath(expansionReq.getQNode());

			if (removedPath) {

				try {

					// VPEvaluator structuredQueryEvaluator = (VPEvaluator)
					// m_session
					// .getStore()
					// .getEvaluator()
					// .getEvaluator(
					// FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);
					//
					// Table<String> expandedTable = structuredQueryEvaluator
					// .evaluate(fquery.getQGraph());

					Table<String> expandedTable = null;

					Result res = new Result(expandedTable);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);

					/*
					 * set facet page
					 */
					FacetPage fpage = m_fpageManager.getInitialFacetPage();
					res.setFacetPage(fpage);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);

					// /*
					// * set res page
					// */
					// resPage = m_cache.getCurrentResultPage(m_session
					// .getCurrentPageNum());

					return res;

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				}

			} else {

				try {

					s_log.error("could not remove node and subtree for node'"
							+ expansionReq.getQNode() + "' not found!");

					return m_session.getCache().getCurrentResult();

					// resPage = m_cache.getCurrentResultPage(m_session
					// .getCurrentPageNum());
					// resPage
					// .setError("could not remove node and subtree for node'"
					// + expansionReq.getQNode() + "' not found!");

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			return resPage;

		} else if (facetRequest instanceof BrowseRequest) {

			// ResultPage resPage = ResultPage.EMPTY_PAGE;
			BrowseRequest browseReq = (BrowseRequest) facetRequest;
			FacetFacetValueTuple newTuple = browseReq.getTuple();

			// try {

			// /*
			// * refine table
			// */
			// Result res = m_cache.getCurrentResult();

			/*
			 * set refined facet page
			 */
			FacetPage fpage = m_fpageManager
					.getRefinedFacetPage(newTuple.getDomain(), newTuple
							.getFacet(), newTuple.getFacetValue());

			// res.setFacetPage(fpage);

			/*
				 * 
				 */
			// m_cache.storeCurrentResult(res);
			// resPage = m_cache.getCurrentResultPage(m_session
			// .getCurrentPageNum());
			return fpage;

			// } catch (DatabaseException e) {
			// e.printStackTrace();
			// } catch (IOException e) {
			// e.printStackTrace();
			// }

			// return resPage;

		} else if (facetRequest instanceof AbstractRefinementRequest) {

			m_session.setCurrentPage(1);
			ResultPage resPage = ResultPage.EMPTY_PAGE;

			if (facetRequest instanceof FacetValueRefinementRequest) {

				FacetValueRefinementRequest refReq = (FacetValueRefinementRequest) facetRequest;
				FacetFacetValueTuple newTuple = refReq.getTuple();

				try {

					StaticNode node = (StaticNode) m_facet2TreeModelConverter
							.facetValue2Node(m_session, newTuple
									.getFacetValue());

					StructuredQuery sQuery = m_facet2QueryModelConverter
							.node2facetFacetValuePath(node);

					List<String> sources = new ArrayList<String>();
					sources.addAll(node.getSources());

					/*
					 * refine table
					 */
					Table<String> refinedTable = m_helper.refineResult(node
							.getDomain(), sources);

					/*
					 * update query
					 */
					FacetedQuery fquery = m_session.getCurrentQuery();
					fquery.clearOldVar2newVarMap();
					fquery.mergeWithAdditionalQuery(node.getDomain(), sQuery);

					m_session.setCurrentQuery(fquery);

					Result res = new Result(refinedTable);
					res.setQuery(fquery);

					/*
					 * clean
					 */

					m_session.clean(CleanType.ALL);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);

					/*
					 * set facet page
					 */
					FacetPage fpage = m_fpageManager.getInitialFacetPage();
					res.setFacetPage(fpage);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);

					return res;
					//
					// resPage = m_cache.getCurrentResultPage(m_session
					// .getCurrentPageNum());

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			} else if (facetRequest instanceof KeywordRefinementRequest) {

				KeywordRefinementRequest keyRefReq = (KeywordRefinementRequest) facetRequest;
				StaticNode facet = (StaticNode) m_treeDelegator.getNode(
						keyRefReq.getDomain(), keyRefReq.getFacetID());

				if (facet.containsDataProperty()) {

					try {

						StructuredQuery sq = new StructuredQuery("sq");
						sq.addEdge(keyRefReq.getDomain(), facet.getValue(),
								keyRefReq.getKeywords());
						sq.setAsSelect(keyRefReq.getDomain());

						VPEvaluator structuredQueryEvaluator = (VPEvaluator) m_session
								.getStore()
								.getEvaluator(m_session)
								.getEvaluator(
										FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);

						Table<String> refinementTable = structuredQueryEvaluator
								.evaluate(sq);

						if (refinementTable.rowCount() > 0) {

							FacetedQuery fquery = m_session.getCurrentQuery();
							fquery.clearOldVar2newVarMap();
							fquery.mergeWithAdditionalQuery(keyRefReq
									.getDomain(), sq);

							// m_helper.updateColumns(refinementTable, fquery);

							List<String> sources = m_helper.getSourcesList(
									refinementTable, keyRefReq.getDomain());

							Table<String> refinedTable = m_helper.refineResult(
									keyRefReq.getDomain(), sources);

							Result res = new Result(refinedTable);

							m_session.setCurrentQuery(fquery);
							res.setQuery(fquery);

							/*
							 * clean
							 */

							m_session.clean(CleanType.ALL);

							/*
							 * store result
							 */
							m_session.getCache().storeCurrentResult(res);

							/*
							 * set facet page
							 */
							FacetPage fpage = m_fpageManager
									.getInitialFacetPage();
							res.setFacetPage(fpage);

							/*
							 * store result
							 */
							m_session.getCache().storeCurrentResult(res);

							return res;

							// resPage = m_cache.getCurrentResultPage(m_session
							// .getCurrentPageNum());

						} else {

							s_log.debug("no results found for query: " + sq);

							Result res = m_session.getCache()
									.getCurrentResult();
							res.setError("no results found for query: " + sq);

							return res;

							// resPage = m_cache.getCurrentResultPage(m_session
							// .getCurrentPageNum());
							//
							// resPage.setError("no results found for query: "
							// + sq);

						}
					} catch (InvalidParameterException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (StorageException e) {
						e.printStackTrace();
					} catch (DatabaseException e) {
						e.printStackTrace();
					}

				} else if (facet.containsObjectProperty()) {

					try {

						StructuredQuery sq = new StructuredQuery("sq");
						sq.addEdge(keyRefReq.getDomain(), facet.getValue(),
								"?tmp");
						sq.setAsSelect(keyRefReq.getDomain());

						KeywordQuery kwq = new KeywordQuery("kwq", keyRefReq
								.getKeywords());

						HybridQuery hq = new HybridQuery("hq", sq, kwq, "?tmp");

						ExploringHybridQueryEvaluator hybridQueryEvaluator = (ExploringHybridQueryEvaluator) m_session
								.getStore()
								.getEvaluator(m_session)
								.getEvaluator(
										FacetEnvironment.EvaluatorType.HybridQueryEvaluator);

						List<TranslatedQuery> translatedQueries = hybridQueryEvaluator
								.evaluate(hq, 1, 1);

						if (!translatedQueries.isEmpty()) {

							Table<String> refinementTable = translatedQueries
									.get(0).getResult();

							if (refinementTable.rowCount() > 0) {

								StructuredQuery sQueryTranslated = translatedQueries
										.get(0);

								FacetedQuery fquery = m_session
										.getCurrentQuery();
								fquery.clearOldVar2newVarMap();
								fquery.mergeWithAdditionalQuery(keyRefReq
										.getDomain(), sQueryTranslated);

								m_session.setCurrentQuery(fquery);

								/*
								 * update columns
								 */
								// m_helper.updateColumns(refinementTable,
								// fquery);
								List<String> sources = m_helper.getSourcesList(
										refinementTable, keyRefReq.getDomain());

								/*
								 * refine result
								 */
								Table<String> refinedTable = m_helper
										.refineResult(keyRefReq.getDomain(),
												sources);
								Result res = new Result(refinedTable);

								/*
								 * clean
								 */
								m_session.clean(CleanType.ALL);

								/*
								 * store result
								 */
								m_session.getCache().storeCurrentResult(res);

								/*
								 * set facet page
								 */
								FacetPage fpage = m_fpageManager
										.getInitialFacetPage();
								res.setFacetPage(fpage);

								return res;

								// resPage = m_cache
								// .getCurrentResultPage(m_session
								// .getCurrentPageNum());
							} else {

								s_log
										.debug("no results found for query: "
												+ hq);

								Result res = m_session.getCache()
										.getCurrentResult();
								res.setError("no results found for query: "
										+ hq);

								return res;

								// resPage = m_cache
								// .getCurrentResultPage(m_session
								// .getCurrentPageNum());
								//
								// resPage.setError("no results found for query: "
								// + hq);

							}
						} else {

							// resPage = m_cache.getCurrentResultPage(m_session
							// .getCurrentPageNum());

							s_log.debug("no translations found for query: "
									+ hq);

							Result res = m_session.getCache()
									.getCurrentResult();
							res.setError("no translations found for query: "
									+ hq);

							return res;

							// resPage
							// .setError("no translations found for query: "
							// + hq);

						}
					} catch (InvalidParameterException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (StorageException e) {
						e.printStackTrace();
					} catch (DatabaseException e) {
						e.printStackTrace();
					}

				} else {
					s_log.error("should not be here node '" + facet + "'");
					return resPage;
				}

			} else {
				s_log.error("facetRequest '" + facetRequest + "'not valid!");
				return resPage;
			}

			return resPage;

		} else {

			s_log.error("facetRequest '" + facetRequest + "'not valid!");
			return ResultPage.EMPTY_PAGE;
		}
	}
}
