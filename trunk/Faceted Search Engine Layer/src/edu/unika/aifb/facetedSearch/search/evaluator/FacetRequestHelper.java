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
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.model.IRefinementPath;
import edu.unika.aifb.facetedSearch.facets.model.impl.QueryRefinementPath;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class FacetRequestHelper {

	private static Logger s_log = Logger.getLogger(FacetRequestHelper.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	public FacetRequestHelper(SearchSession session) {
		m_session = session;
	}

	public IRefinementPath query2facetFacetValuePath(String domain, Query query) {

		if (query instanceof StructuredQuery) {

			return new QueryRefinementPath(domain, (StructuredQuery) query);

		} else {
			s_log.error("should not be here: query '" + query + "'");
			return null;
		}
	}

	public Table<String> refineResult(List<String> nodeSources, String domain)
			throws DatabaseException, IOException {

		Table<String> oldTable = m_session.getCache().getCurrentResultTable();
		oldTable.sort(domain, true);

		Collections.sort(nodeSources);

		return FacetUtils.mergeJoin(oldTable, nodeSources, domain);
	}

	public Result refineResult(Table<String> refinementTable, String domain) {

		Result res = new Result();
		refinementTable.sort(domain);

		try {

			Table<String> oldTable = m_session.getCache()
					.getCurrentResultTable();
			oldTable.sort(domain, true);

			res.setResultTable(Tables.mergeJoin(oldTable, refinementTable,
					domain));

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return res;
	}

	public void updateColumns(Table<String> newTable, FacetedQuery fquery) {

		String[] names = newTable.getColumnNames();

		for (String name : names) {

			if (fquery.getOldVar2newVarMap().containsKey(name)) {

				String newName = fquery.getOldVar2newVarMap().get(name);
				newTable.setColumnName(newTable.getColumn(name), newName);
			}
		}
	}

	public void cleanQuery(StructuredQuery sQuery, FacetedQuery fQuery) {

		List<QueryEdge> egdes2GenericNodes = fQuery.getEdges2GenericNodes();

		for (QueryEdge edge : egdes2GenericNodes) {
			sQuery.getQueryGraph().removeEdge(edge);
		}
	}
}
