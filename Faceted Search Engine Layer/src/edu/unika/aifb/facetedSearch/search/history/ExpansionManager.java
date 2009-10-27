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

import java.util.Collection;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueTuple;
import edu.unika.aifb.facetedSearch.index.db.StorageHelperThread;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class ExpansionManager {

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private StoredMap<Double, String> m_sources4ffvTupleMap;

	/*
	 * binding
	 */
	private EntryBinding<Double> m_doubleBinding;
	private EntryBinding<String> m_strgBinding;

	public ExpansionManager(SearchSession session) {

		m_session = session;
		init();
	}

	public void clean() {
		m_sources4ffvTupleMap.clear();
	}

	public void close() {

		clean();
		m_sources4ffvTupleMap = null;
		m_strgBinding = null;
	}

	public Collection<String> getSources(FacetFacetValueTuple ffvTuple) {
		return m_sources4ffvTupleMap.duplicates(ffvTuple.getId());
	}

	private void init() {

		try {

			new StoredClassCatalog(m_session.getCache().getDB(
					FacetEnvironment.DatabaseName.CLASS));

			/*
			 * bindings
			 */
			m_doubleBinding = TupleBinding.getPrimitiveBinding(Double.class);
			m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

			/*
			 * stored map
			 */
			m_sources4ffvTupleMap = new StoredMap<Double, String>(
					m_session.getCache().getDB(
							FacetEnvironment.DatabaseName.FEXP_CACHE),
					m_doubleBinding, m_strgBinding, true);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public boolean isOpen() {
		return m_sources4ffvTupleMap != null;
	}

	public void putSources(FacetFacetValueTuple ffvTuple, List<String> sources) {

		StorageHelperThread<Double, String> storageHelper = new StorageHelperThread<Double, String>(
				m_sources4ffvTupleMap, ffvTuple.getId(), sources);
		storageHelper.start();
	}
}
