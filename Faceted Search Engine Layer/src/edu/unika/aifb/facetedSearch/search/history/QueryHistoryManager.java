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
package edu.unika.aifb.facetedSearch.search.history;

import org.apache.jcs.access.exception.CacheException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.CleanType;

/**
 * @author andi
 * 
 */
public class QueryHistoryManager {

	/*
	 * 
	 */
	private static QueryHistoryManager s_instance;

	public static QueryHistoryManager getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new QueryHistoryManager(
				session) : s_instance;
	}

	/*
	 * map
	 */
	private StoredMap<Double, Result> m_history;

	/*
	 * bindings
	 */
	private EntryBinding<Result> m_resBinding;
	private EntryBinding<Double> m_doubleBinding;

	/*
	 * session stuff
	 */
	@SuppressWarnings("unused")
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	private QueryHistoryManager(SearchSession session) {

		m_session = session;
		m_cache = session.getCache();

		try {

			init();

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void clean() throws DatabaseException, CacheException {

		m_cache.clean(CleanType.HISTORY);
		reOpen();
	}

	public void close() throws DatabaseException, CacheException {

		m_cache.clean(CleanType.HISTORY);
		m_history = null;
		System.gc();
	}

	public boolean containsResult(double queryID) {
		return m_history.containsKey(queryID);
	}

	public Result getResult(double queryID) {
		return m_history.get(queryID);
	}

	private void init() throws IllegalArgumentException, DatabaseException {

		StoredClassCatalog cata = new StoredClassCatalog(m_cache
				.getDB(FacetEnvironment.DatabaseName.CLASS));

		/*
		 * bindings
		 */
		m_resBinding = new SerialBinding<Result>(cata, Result.class);
		m_doubleBinding = TupleBinding.getPrimitiveBinding(Double.class);

		/*
		 * stored map
		 */
		m_history = new StoredMap<Double, Result>(m_cache
				.getDB(FacetEnvironment.DatabaseName.FHIST_CACHE),
				m_doubleBinding, m_resBinding, true);

	}

	public boolean isOpen() {
		return m_history != null;
	}

	public boolean putResult(double queryID, Result res) {
		return m_history.put(queryID, res) == null ? true : false;
	}

	public void reOpen() {

		if (m_history == null) {

			m_history = new StoredMap<Double, Result>(m_cache
					.getDB(FacetEnvironment.DatabaseName.FHIST_CACHE),
					m_doubleBinding, m_resBinding, true);
		}
	}
}
