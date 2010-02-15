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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.index.db.StorageHelperThread;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class HistoryManager {

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private StoredMap<String, Result> m_history;

	/*
	 * bindings
	 */
	private EntryBinding<Result> m_resBinding;
	private EntryBinding<String> m_strgBinding;

	public HistoryManager(SearchSession session) {

		m_session = session;
		init();
	}

	public void clean() {
		m_history.clear();
	}

	public void close() {

		clean();
		m_resBinding = null;
		m_strgBinding = null;
	}

	public Result getResult(String query) {
		return m_history.get(query);
	}

	public boolean hasResult(String query) {
		return m_history.containsKey(query);
	}

	private void init() {

		try {

			StoredClassCatalog cata = new StoredClassCatalog(m_session
					.getCache().getDB(FacetEnvironment.DatabaseName.CLASS));

			/*
			 * bindings
			 */
			m_resBinding = new SerialBinding<Result>(cata, Result.class);
			m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

			/*
			 * stored map
			 */
			m_history = new StoredMap<String, Result>(m_session.getCache()
					.getDB(FacetEnvironment.DatabaseName.FHIST_CACHE),
					m_strgBinding, m_resBinding, true);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public boolean isOpen() {
		return m_history != null;
	}

	public synchronized void putResult(String query, Result res) {

		StorageHelperThread<String, Result> storageHelper = new StorageHelperThread<String, Result>(
				m_history, query, res);
		storageHelper.start();
	}
}