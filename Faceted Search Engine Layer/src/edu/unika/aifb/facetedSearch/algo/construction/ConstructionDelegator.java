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
package edu.unika.aifb.facetedSearch.algo.construction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.FacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class ConstructionDelegator extends Delegator {

	private SearchSessionCache m_sessionCache;
	private SearchSession m_session;

	private static ConstructionDelegator s_instance;
	private static Logger s_log = Logger.getLogger(ResultPage.class);

	public static ConstructionDelegator getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new ConstructionDelegator(
				session) : s_instance;
	}

	private ConstructionDelegator(SearchSession session) {
		m_session = session;
	}

	public void clean() {

	}

	public HashMap<String, HashMap<String, ArrayList<String>>> doFacetConstruction(
			Table<String> results) throws EnvironmentLockedException,
			IOException, DatabaseException, StorageException {

		s_log.debug("start facet construction for new result set");

		m_sessionCache = m_session.getCache();

		FacetTreeDelegator treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
		treeDelegator.clean();

		for (String colName : results.getColumnNames()) {

			s_log.debug("start building facet tree for column '" + colName
					+ "'");

			IFacetTreeBuilder builder = new FacetTreeBuilder(m_session);
			FacetTree tree = builder.contruct(results, results
					.getColumn(colName));
			treeDelegator.addTree4Domain(colName, tree);

			s_log.debug("finished facet tree for column '" + colName + "'!");
		}

		// TODO

		return null;
	}

}
