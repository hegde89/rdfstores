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
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.converter.facet2tree.Facet2TreeModelConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValueDummy;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractRefinementRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.BrowseRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.ExpansionRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.FacetValueRefinementRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.HistoryFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.KeywordRefinementRequest;
import edu.unika.aifb.facetedSearch.search.evaluator.helper.FacetRefinementHelper;
import edu.unika.aifb.facetedSearch.search.fpage.FacetPageManager;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.CleanType;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Converters;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.SessionStatus;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.util.Util;

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

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * 
	 */
	private FacetRefinementHelper m_helper;

	public FacetRequestEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);

		m_session = session;

		m_facet2TreeModelConverter = (Facet2TreeModelConverter) session
				.getConverter(Converters.FACET2TREE);

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
		m_helper = new FacetRefinementHelper(session);
		m_fpageManager = session.getFacetPageManager();

	}

	public Object evaluate(AbstractFacetRequest facetRequest) {

		if (facetRequest instanceof HistoryFacetRequest) {

			HistoryFacetRequest histReq = (HistoryFacetRequest) facetRequest;

			if ((histReq.getAbstractQuery() != null)
					&& m_session.getHistoryManager().hasResult(
							histReq.getAbstractQuery())) {

				m_session.clean(CleanType.REFINEMENT);

				String historyQuery = histReq.getAbstractQuery();
				Result res = m_session.getHistoryManager().getResult(
						historyQuery);

				FacetedQuery fQuery = res.getQuery();
				res.setQuery(fQuery);

				m_session.setCurrentQuery(fQuery);

				try {

					FacetPage fpage = m_fpageManager.getInitialFacetPage(res
							.getResultTable());
					res.setFacetPage(fpage);

					m_session.getCache().storeCurrentResult(res);
					m_session.setStatus(SessionStatus.FREE);

					return res;

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				}

				res = new Result();
				res.setFacetPage(new FacetPage());
				m_session.setStatus(SessionStatus.FREE);

				return res;

			} else {

				Result res = new Result();
				res.setFacetPage(new FacetPage());
				m_session.setStatus(SessionStatus.FREE);

				return res;
			}
		} else if (facetRequest instanceof ExpansionRequest) {

			m_session.setCurrentPage(1);

			ExpansionRequest expansionReq = (ExpansionRequest) facetRequest;
			FacetFacetValueTuple ffvTuple = expansionReq.getFfvTuple();

			FacetedQuery fQuery = m_session.getCurrentQuery();

			if (fQuery.containsFacetFacetValueTuple(ffvTuple)) {

				try {

					fQuery.removeFacetFacetValueTuple(ffvTuple);

					Result initRes = m_session.getHistoryManager().getResult(
							FacetEnvironment.DefaultValue.INIT_QUERY_NAME);

					Table<String> resTable = initRes.getResultTable();

					for (FacetFacetValueTuple nextFfvTuple : fQuery
							.getFacetFacetValueList()) {

						List<String> sources = new ArrayList<String>();
						sources.addAll(m_session.getExpansionManagerManager()
								.getSources(nextFfvTuple));

						resTable = m_helper.refineResult(nextFfvTuple
								.getDomain(), sources, resTable);

					}

					m_session.clean(CleanType.EXPANSION);

					Result res = new Result(resTable);
					res.setQuery(fQuery);

					FacetPage fpage = m_fpageManager
							.getInitialFacetPage(resTable);
					res.setFacetPage(fpage);

					m_session.getCache().storeCurrentResult(res);
					m_session.setStatus(SessionStatus.FREE);

					return res;

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				Result res = new Result();
				res.setFacetPage(new FacetPage());
				m_session.setStatus(SessionStatus.FREE);

				return res;

			} else {

				Result res = new Result();
				res.setFacetPage(new FacetPage());
				m_session.setStatus(SessionStatus.FREE);

				return res;
			}
		} else if (facetRequest instanceof BrowseRequest) {

			m_session.touch();

			BrowseRequest browseReq = (BrowseRequest) facetRequest;
			FacetFacetValueTuple newTuple = browseReq.getTuple();

			/*
			 * set refined facet page
			 */
			FacetPage fpage = m_fpageManager
					.getRefinedFacetPage(newTuple.getDomain(), newTuple
							.getFacet(), newTuple.getFacetValue());

			return fpage;

		} else if (facetRequest instanceof AbstractRefinementRequest) {

			m_session.setCurrentPage(1);

			if (facetRequest instanceof FacetValueRefinementRequest) {

				FacetValueRefinementRequest refReq = (FacetValueRefinementRequest) facetRequest;
				FacetFacetValueTuple newTuple = refReq.getTuple();

				try {

					Node node = m_facet2TreeModelConverter.facetValue2Node(
							m_session, newTuple.getFacetValue());

					List<String> sources = new ArrayList<String>();
					sources.addAll(m_helper.getCleanCollection(m_session
							.getCache().getSources4Node(node)));

					/*
					 * 
					 */
					m_session.getExpansionManagerManager().putSources(newTuple,
							sources);

					Result oldRes = m_session.getCache().getCurrentResult();

					/*
					 * refine table
					 */
					Table<String> refinedTable = m_helper.refineResult(node
							.getDomain(), sources, oldRes.getResultTable());

					/*
					 * update query
					 */
					FacetedQuery fquery = m_session.getCurrentQuery();
					String nextAbstractQuery = fquery
							.addFacetFacetValueTuple(newTuple);

					m_session.setCurrentQuery(fquery);

					Result res = new Result(refinedTable);
					res.setQuery(fquery);

					/*
					 * clean
					 */

					m_session.clean(CleanType.REFINEMENT);

					/*
					 * set facet page
					 */
					FacetPage fpage = m_fpageManager
							.getInitialFacetPage(refinedTable);
					res.setFacetPage(fpage);

					/*
					 * store result
					 */
					m_session.getCache().storeCurrentResult(res);
					m_session.getHistoryManager().putResult(nextAbstractQuery,
							res);
					m_session.setStatus(SessionStatus.FREE);

					return res;

				} catch (DatabaseException e) {

					e.printStackTrace();
					m_session.setStatus(SessionStatus.FREE);

				} catch (IOException e) {

					e.printStackTrace();
					m_session.setStatus(SessionStatus.FREE);

				}

			} else if (facetRequest instanceof KeywordRefinementRequest) {

				KeywordRefinementRequest keyRefReq = (KeywordRefinementRequest) facetRequest;
				StaticNode facet = (StaticNode) m_treeDelegator.getNode(
						keyRefReq.getDomain(), keyRefReq.getFacetID());

				if (facet.containsDataProperty()) {

					StructuredQuery sq = new StructuredQuery("sq");
					sq.addEdge(keyRefReq.getDomain(), facet.getValue(),
							keyRefReq.getKeywords());
					sq.setAsSelect(keyRefReq.getDomain());

					Table<String> refinementTable = null;

					try {

						VPEvaluator structuredQueryEvaluator = (VPEvaluator) m_session
								.getStore()
								.getEvaluator(m_session)
								.getEvaluator(
										FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);

						refinementTable = structuredQueryEvaluator.evaluate(sq);

					} catch (Exception e) {

						s_log.error("error while keyword refinement request"
								+ e.getMessage());

						refinementTable = null;
					}

					if ((refinementTable != null)
							&& (refinementTable.rowCount() > 0)) {

						FacetFacetValueTuple ffvTuple = new FacetFacetValueTuple();
						ffvTuple.setDomain(keyRefReq.getDomain());
						ffvTuple.setFacet(new Facet(facet.getValue()));
						ffvTuple.setFacetValue(new FacetValueDummy(keyRefReq
								.getKeywords()));

						FacetedQuery fquery = m_session.getCurrentQuery();
						String nextAbstractQuery = fquery
								.addFacetFacetValueTuple(ffvTuple);

						List<String> sources = m_helper.getSourcesList(
								refinementTable, keyRefReq.getDomain());

						m_session.getExpansionManagerManager().putSources(
								ffvTuple, sources);

						try {

							Result oldRes = m_session.getCache()
									.getCurrentResult();

							Table<String> refinedTable = m_helper.refineResult(
									keyRefReq.getDomain(), sources, oldRes
											.getResultTable());

							Result newRes = new Result(refinedTable);

							m_session.setCurrentQuery(fquery);
							newRes.setQuery(fquery);

							/*
							 * clean
							 */

							m_session.clean(CleanType.REFINEMENT);

							/*
							 * set facet page
							 */
							FacetPage fpage = m_fpageManager
									.getInitialFacetPage(refinedTable);
							newRes.setFacetPage(fpage);

							/*
							 * store result
							 */
							m_session.getCache().storeCurrentResult(newRes);
							m_session.getHistoryManager().putResult(
									nextAbstractQuery, newRes);
							m_session.setStatus(SessionStatus.FREE);

							return newRes;

						} catch (DatabaseException e) {

							s_log.error("DatabaseException: " + e.getMessage());

						} catch (UnsupportedEncodingException e) {

							s_log.error("UnsupportedEncodingException: "
									+ e.getMessage());

						} catch (IOException e) {
							s_log.error("IOException: " + e.getMessage());

						} catch (Exception e) {
							s_log.error("Other Exception: " + e.getMessage());
						}

						Result res = new Result();
						res.setFacetPage(new FacetPage());
						m_session.setStatus(SessionStatus.FREE);

						return res;

					} else {

						List<String> sources = new ArrayList<String>();

						if ((refinementTable.rowCount() == 0)) {

							Iterator<String> subjIter = m_session.getCache()
									.getSubjects4Node(facet).iterator();

							while (subjIter.hasNext()) {

								String subject = subjIter.next();

								Iterator<AbstractSingleFacetValue> objIter = m_session
										.getCache().getObjects4StaticNode(
												facet, subject).iterator();

								while (objIter.hasNext()) {

									AbstractSingleFacetValue fv = objIter
											.next();

									if (fv instanceof Literal) {

										String litValue = ((Literal) fv)
												.getLiteralValue();

										if (litValue.equals(keyRefReq
												.getKeywords())) {

											sources.add(FacetUtils
													.decodeLocalName(subject));
										}
									}
								}
							}
						}

						if (!sources.isEmpty()) {

							try {

								FacetFacetValueTuple ffvTuple = new FacetFacetValueTuple();
								ffvTuple.setDomain(keyRefReq.getDomain());
								ffvTuple.setFacet(new Facet(facet.getValue()));
								ffvTuple.setFacetValue(new FacetValueDummy(
										keyRefReq.getKeywords()));

								m_session.getExpansionManagerManager()
										.putSources(ffvTuple, sources);

								FacetedQuery fquery = m_session
										.getCurrentQuery();
								String nextAbstractQuery = fquery
										.addFacetFacetValueTuple(ffvTuple);

								m_session.setCurrentQuery(fquery);

								Result oldRes = m_session.getCache()
										.getCurrentResult();

								Table<String> refinedTable = m_helper
										.refineResult(keyRefReq.getDomain(),
												sources, oldRes
														.getResultTable());

								Result newRes = new Result(refinedTable);
								newRes.setQuery(fquery);

								/*
								 * clean
								 */

								m_session.clean(CleanType.REFINEMENT);

								/*
								 * set facet page
								 */
								FacetPage fpage = m_fpageManager
										.getInitialFacetPage(refinedTable);
								newRes.setFacetPage(fpage);

								/*
								 * store result
								 */
								m_session.getCache().storeCurrentResult(newRes);
								m_session.getHistoryManager().putResult(
										nextAbstractQuery, newRes);
								m_session.setStatus(SessionStatus.FREE);

								return newRes;

							} catch (DatabaseException e) {

								s_log.error("DatabaseException: "
										+ e.getMessage());

							} catch (UnsupportedEncodingException e) {

								s_log.error("UnsupportedEncodingException: "
										+ e.getMessage());

							} catch (IOException e) {

								s_log.error("IOException: " + e.getMessage());

							} catch (Exception e) {

								s_log.error("Other Exception: "
										+ e.getMessage());

							}

							Result res = new Result();
							res.setFacetPage(new FacetPage());
							m_session.setStatus(SessionStatus.FREE);

							return res;

						}

						s_log.debug("no results found for query: " + sq);

						Result res = new Result();
						res.setFacetPage(new FacetPage());
						m_session.setStatus(SessionStatus.FREE);

						return res;
					}

				} else if (facet.containsRdfProperty()) {

					String object;

					if (Util.isEntity(keyRefReq.getKeywords())) {

						object = FacetUtils.cleanURI(keyRefReq.getKeywords());

					} else {

						object = FacetEnvironment.DefaultValue.NAMESPACE
								+ keyRefReq.getKeywords();
					}

					try {

						Node node = ((FacetTreeDelegator) m_session
								.getDelegator(Delegators.TREE)).getTree(
								keyRefReq.getDomain()).getVertex(object);

						if (node != null) {

							List<String> sources = new ArrayList<String>();
							sources.addAll(m_helper
									.getCleanCollection(m_session.getCache()
											.getSubjects4Node(node)));

							if (!sources.isEmpty()) {

								FacetFacetValueTuple ffvTuple = new FacetFacetValueTuple();
								ffvTuple.setDomain(keyRefReq.getDomain());
								ffvTuple.setFacet(new Facet(facet.getValue()));
								ffvTuple.setFacetValue(new FacetValueDummy(
										keyRefReq.getKeywords()));

								m_session.getExpansionManagerManager()
										.putSources(ffvTuple, sources);

								Result oldRes = m_session.getCache()
										.getCurrentResult();

								/*
								 * refine table
								 */
								Table<String> refinedTable = m_helper
										.refineResult(keyRefReq.getDomain(),
												sources, oldRes
														.getResultTable());

								/*
								 * update query
								 */
								FacetedQuery fquery = m_session
										.getCurrentQuery();
								String nextAbstractQuery = fquery
										.addFacetFacetValueTuple(ffvTuple);
								m_session.setCurrentQuery(fquery);

								Result newRes = new Result(refinedTable);
								newRes.setQuery(fquery);

								/*
								 * clean
								 */

								m_session.clean(CleanType.REFINEMENT);

								/*
								 * set facet page
								 */
								FacetPage fpage = m_fpageManager
										.getInitialFacetPage(refinedTable);
								newRes.setFacetPage(fpage);

								/*
								 * store result
								 */
								m_session.getCache().storeCurrentResult(newRes);
								m_session.getHistoryManager().putResult(
										nextAbstractQuery, newRes);
								m_session.setStatus(SessionStatus.FREE);

								return newRes;

							} else {

								Result res = new Result();
								res.setFacetPage(new FacetPage());
								m_session.setStatus(SessionStatus.FREE);

								return res;
							}
						} else {

							Result res = new Result();
							res.setFacetPage(new FacetPage());
							m_session.setStatus(SessionStatus.FREE);

							return res;
						}
					} catch (Exception e) {

						s_log.debug("error: " + e.getMessage());

						Result res = new Result();
						res.setFacetPage(new FacetPage());
						m_session.setStatus(SessionStatus.FREE);

						return res;
					}
				} else if (facet.containsObjectProperty()) {

					if (Util.isEntity(keyRefReq.getKeywords())) {

						try {

							FacetFacetValueTuple ffvTuple = new FacetFacetValueTuple();
							ffvTuple.setDomain(keyRefReq.getDomain());
							ffvTuple.setFacet(new Facet(facet.getValue()));
							ffvTuple.setFacetValue(new FacetValueDummy(
									keyRefReq.getKeywords()));

							String object = FacetUtils.cleanURI(keyRefReq
									.getKeywords());
							object = FacetUtils.decodeLocalName(object);

							List<String> sources = new ArrayList<String>();
							sources.addAll(m_helper
									.getCleanCollection(m_session.getCache()
											.getSources4Object(
													keyRefReq.getDomain(),
													object)));

							m_session.getExpansionManagerManager().putSources(
									ffvTuple, sources);

							Result oldRes = m_session.getCache()
									.getCurrentResult();

							/*
							 * refine table
							 */
							Table<String> refinedTable = m_helper.refineResult(
									keyRefReq.getDomain(), sources, oldRes
											.getResultTable());

							/*
							 * update query
							 */
							FacetedQuery fquery = m_session.getCurrentQuery();
							String nextAbstractQuery = fquery
									.addFacetFacetValueTuple(ffvTuple);

							m_session.setCurrentQuery(fquery);

							Result newRes = new Result(refinedTable);
							newRes.setQuery(fquery);

							/*
							 * clean
							 */
							m_session.clean(CleanType.REFINEMENT);

							/*
							 * set facet page
							 */
							FacetPage fpage = m_fpageManager
									.getInitialFacetPage(refinedTable);
							newRes.setFacetPage(fpage);

							/*
							 * store result
							 */
							m_session.getCache().storeCurrentResult(newRes);
							m_session.getHistoryManager().putResult(
									nextAbstractQuery, newRes);
							m_session.setStatus(SessionStatus.FREE);

							return newRes;

						} catch (Exception e) {

							s_log.debug("error: " + e.getMessage());

							Result res = new Result();
							res.setFacetPage(new FacetPage());
							m_session.setStatus(SessionStatus.FREE);

							return res;
						}
					} else {

						try {

							StructuredQuery sq = new StructuredQuery("sq");
							sq.addEdge(keyRefReq.getDomain(), facet.getValue(),
									"?tmp");
							sq.addEdge("?tmp", RDFS.LABEL.stringValue(),
									keyRefReq.getKeywords());

							Table<String> refinementTable = null;

							try {

								VPEvaluator structuredQueryEvaluator = (VPEvaluator) m_session
										.getStore()
										.getEvaluator(m_session)
										.getEvaluator(
												FacetEnvironment.EvaluatorType.StructuredQueryEvaluator);

								refinementTable = structuredQueryEvaluator
										.evaluate(sq);

							} catch (Exception e) {

								s_log
										.error("error while keyword refinement request"
												+ e.getMessage());

								refinementTable = null;
							}

							if ((refinementTable != null)
									&& (refinementTable.rowCount() > 0)) {

								FacetFacetValueTuple ffvTuple = new FacetFacetValueTuple();
								ffvTuple.setDomain(keyRefReq.getDomain());
								ffvTuple.setFacet(new Facet(facet.getValue()));
								ffvTuple.setFacetValue(new FacetValueDummy(
										keyRefReq.getKeywords()));

								FacetedQuery fquery = m_session
										.getCurrentQuery();
								String nextAbstractQuery = fquery
										.addFacetFacetValueTuple(ffvTuple);

								m_session.setCurrentQuery(fquery);

								List<String> sources = m_helper.getSourcesList(
										refinementTable, keyRefReq.getDomain());

								m_session.getExpansionManagerManager()
										.putSources(ffvTuple, sources);

								Result oldRes = m_session.getCache()
										.getCurrentResult();

								/*
								 * refine result
								 */
								Table<String> refinedTable = m_helper
										.refineResult(keyRefReq.getDomain(),
												sources, oldRes
														.getResultTable());

								Result newRes = new Result(refinedTable);
								newRes.setQuery(fquery);

								/*
								 * clean
								 */
								m_session.clean(CleanType.REFINEMENT);

								/*
								 * set facet page
								 */
								FacetPage fpage = m_fpageManager
										.getInitialFacetPage(refinedTable);
								newRes.setFacetPage(fpage);

								m_session.getCache().storeCurrentResult(newRes);

								m_session.getHistoryManager().putResult(
										nextAbstractQuery, newRes);

								m_session.setStatus(SessionStatus.FREE);

								return newRes;

							} else {

								s_log
										.debug("no results found for query: "
												+ sq);

								Result res = new Result();
								res.setFacetPage(new FacetPage());
								m_session.setStatus(SessionStatus.FREE);

								return res;

							}
						} catch (InvalidParameterException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (DatabaseException e) {
							e.printStackTrace();
						}
					}
				} else {

					s_log.error("should not be here node '" + facet + "'");

					Result res = new Result();
					res.setFacetPage(new FacetPage());
					m_session.setStatus(SessionStatus.FREE);

					return res;
				}

			} else {

				s_log.error("facetRequest '" + facetRequest + "'not valid!");

				Result res = new Result();
				res.setFacetPage(new FacetPage());
				m_session.setStatus(SessionStatus.FREE);

				return res;
			}

			s_log.error("facetRequest '" + facetRequest + "'not valid!");

			Result res = new Result();
			res.setFacetPage(new FacetPage());
			m_session.setStatus(SessionStatus.FREE);

			return res;

		} else {

			s_log.error("facetRequest '" + facetRequest + "'not valid!");

			Result res = new Result();
			res.setFacetPage(new FacetPage());
			m_session.setStatus(SessionStatus.FREE);

			return res;
		}
	}
}