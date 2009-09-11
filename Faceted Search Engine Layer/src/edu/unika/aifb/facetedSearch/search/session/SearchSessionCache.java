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
package edu.unika.aifb.facetedSearch.search.session;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.DbConfigFactory;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.EnvironmentFactory;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 * 
 */
public class SearchSessionCache {

	private class Keys {
		
		public static final String RESULT_SET = "res";
		public static final String FACET_TREE = "tree";
		
	}

	private Environment m_env;
	private Database m_cache;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		// init db
		m_env = EnvironmentFactory.make(dir);

		m_cache = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FDB_CACHE, DbConfigFactory
						.make(false));

	}

	public void clear() throws DatabaseException {

		if (m_cache != null) {
			m_cache.close();
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FDB_CACHE);
		}

		m_cache = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FDB_CACHE, DbConfigFactory
						.make(true));
	}

	@SuppressWarnings("unchecked")
	public Table<String> getResults4Page(int page) throws DatabaseException,
			IOException {

		int fromIndex = 0;
		Table<String> res = (Table<String>) FacetDbUtils.get(m_cache,
				Keys.RESULT_SET);

		if ((fromIndex = page
				* FacetEnvironment.DefaultValue.NUMBER_OF_RESULTS_PER_PAGE) > res
				.size()) {

			return null;

		} else {

			int toIndex = Math.min((page + 1)
					* FacetEnvironment.DefaultValue.NUMBER_OF_RESULTS_PER_PAGE,
					res.size());

			return res.subTable(fromIndex, toIndex);
		}
	}

	public void storeResultSet(Table<String> res) {

		FacetDbUtils.store(m_cache, Keys.RESULT_SET, res);
	}
}
