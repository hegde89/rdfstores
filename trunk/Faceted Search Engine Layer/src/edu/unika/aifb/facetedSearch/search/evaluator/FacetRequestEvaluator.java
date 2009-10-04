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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.converter.facet2tree.Facet2TreeModelConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.FacetPageManager;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.AbstractFacetRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.ExpansionRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.request.RefinementRequest;
import edu.unika.aifb.facetedSearch.search.history.QueryHistoryManager;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Converters;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.Searcher;

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
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private FacetPageManager m_fpageManager;
	private QueryHistoryManager m_history;

	/*
	 * 
	 */
	private Facet2TreeModelConverter m_facet2TreeModelConverter;

	public FacetRequestEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);

		m_session = session;
		m_cache = session.getCache();

		m_history = session.getHistory();
		m_fpageManager = session.getFacetPageManager();

		m_facet2TreeModelConverter = (Facet2TreeModelConverter) session
				.getConverter(Converters.FACET2TREE);

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

					resPage = m_cache.getResultPage(m_session
							.getCurrentPageNum());

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
			FacetFacetValueTuple newTuple = refReq.getTuple();

			try {

				/*
				 * refine table
				 */
				Result res = refineTable(newTuple.getFacetValue());

				/*
				 * update query
				 */
				FacetedQuery fquery = m_session.getCurrentQuery();
				fquery.addFacetFacetValueTuple(newTuple);

				m_session.setCurrentQuery(fquery);

				/*
				 * set refined facet page
				 */
				FacetPage fpage = m_fpageManager.getRefinedFacetPage(newTuple
						.getDomain(), newTuple.getFacet(), newTuple
						.getFacetValue());

				res.setFacetPage(fpage);

				
				/*
				 * store result & update history
				 */
				m_cache.storeResult(res);
				m_history.putQueryResultTuple(fquery.getId(), res);

			} catch (DatabaseException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {

				return m_cache.getResultPage(m_session.getCurrentPageNum());

			} catch (DatabaseException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return ResultPage.EMPTY_PAGE;

		} else {
			s_log.error("facetRequest '" + facetRequest + "'not valid!");
			return ResultPage.EMPTY_PAGE;
		}
	}
	private Table<String> mergeJoin(Table<String> left, List<String> right,
			String col) throws UnsupportedOperationException {

		if (!left.isSorted() || !left.getSortedColumn().equals(col)) {
			throw new UnsupportedOperationException(
					"merge join with unsorted tables");
		}

		List<String> resultColumns = new ArrayList<String>();
		resultColumns.add(col);

		for (String strg : left.getColumnNames()) {
			if (!strg.equals(col)) {
				resultColumns.add(strg);
			}
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

	private Result refineTable(AbstractFacetValue fv) throws DatabaseException,
			IOException {

		StaticNode node = (StaticNode) m_facet2TreeModelConverter
				.facetValue2Node(fv);

		Table<String> oldTable = m_session.getCache().getResultTable();
		oldTable.sort(fv.getDomain(), true);

		List<String> sourceIndividuals = new ArrayList<String>(node
				.getSources());
		Collections.sort(sourceIndividuals);

		Result res = new Result();
		res.setResultTable(mergeJoin(oldTable, sourceIndividuals, fv
				.getDomain()));

		return res;
	}
}
