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
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.PreloadConfig;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 * 
 */
public class SearchSessionCache {

	private class Keys {
		public static final String RESULT_SET = "re";
		public static final String ROOT = "ro";
		public static final String RANGE_ROOT = "raro";
	}

	private File m_dir;

	private Environment m_env;
	private Database m_resCache;
	private Database m_objectCache;
	private Database m_subjectCache;
	private Database m_pathCache;

	@SuppressWarnings("unchecked")
	private EntryBinding<Table> m_resBinding;
	private EntryBinding<String> m_objBinding;
	private EntryBinding<String> m_subjBinding;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		m_dir = dir;
		open();

	}

	public void addObjects(List<String> objects, double nodeId)
			throws UnsupportedEncodingException {

		String key = FacetDbUtils
				.getKey(new String[] { String.valueOf(nodeId) });

		for (String object : objects) {

			FacetDbUtils.store(m_objectCache, key, object, m_objBinding);

		}
	}

	// public void addObjects(String objectsStrg, double nodeId) {
	//
	// List<String> objectsList = FacetUtils.string2List(objectsStrg);
	//
	// String key = FacetDbUtils
	// .getKey(new String[] { String.valueOf(nodeId) });
	//
	// for (String object : objectsList) {
	//
	// FacetDbUtils.store(m_objectCache, key, object);
	//
	// }
	// }

	public void addSourceIndivdual(String ind, double nodeId)
			throws UnsupportedEncodingException {

		FacetDbUtils.store(m_subjectCache, FacetDbUtils
				.getKey(new String[] { String.valueOf(nodeId) }), ind,
				m_subjBinding);

	}

	public void clear() throws DatabaseException {

		if (m_resCache != null) {

			m_resCache.close();
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FRES_CACHE);
		}

		if (m_subjectCache != null) {

			m_subjectCache.close();
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FS_CACHE);
		}

		if (m_objectCache != null) {

			m_objectCache.close();
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FO_CACHE);
		}

		if (m_pathCache != null) {

			m_pathCache.close();
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FP_CACHE);
		}

		open();
	}

	public void close() throws DatabaseException {

		clear();

		if (m_env != null) {
			m_env.close();
		}

		m_resCache = null;
		m_objectCache = null;
		m_subjectCache = null;
		m_pathCache = null;

		m_env = null;
	}

	// public List<Edge> getAncestorPath2RangeRoot(FacetTree tree, double
	// nodeId)
	// throws DatabaseException, IOException {
	//
	// // String key = FacetDbUtils.getKey(new String[] { Keys.RANGE_ROOT,
	// // String.valueOf(tree.getId()), String.valueOf(node.getID()) });
	// //
	// // GraphPath<Node, Edge> path = null;
	//
	// // if ((path = FacetDbUtils.get(m_pathCache, key)) == null) {
	// //
	// // path = tree.getAncestorPath2RangeRoot(node);
	// // FacetDbUtils.store(m_pathCache, key, path);
	// //
	// // }
	//
	// return tree.getAncestorPath2RangeRoot(nodeId).getEdgeList();
	// }

	// public Stack<Edge> getAncestorPath2Root(FacetTree tree, double nodeId)
	// throws DatabaseException, IOException {
	//
	// // String key = FacetDbUtils.getKey(new String[] { Keys.ROOT,
	// // String.valueOf(tree.getId()), String.valueOf(node.getID()) });
	// //
	// // GraphPath<Node, Edge> path = null;
	// //
	// // if ((path = FacetDbUtils.get(m_pathCache, key)) == null) {
	// //
	// // path = tree.getAncestorPath2Root(node);
	// // FacetDbUtils.store(m_pathCache, key, path);
	// //
	// // }
	//
	// return tree.getAncestorPath2Root(nodeId).getEdgeList();
	// }

	public HashSet<String> getObjects(double nodeID) throws DatabaseException,
			IOException {

		return FacetDbUtils.getAllAsSet(m_objectCache, FacetDbUtils
				.getKey(new String[] { String.valueOf(nodeID) }), m_objBinding);
	}

	@SuppressWarnings("unchecked")
	public Table<String> getResults4Page(int page) throws DatabaseException,
			IOException {

		int fromIndex;
		Table<String> res = FacetDbUtils.get(m_resCache, Keys.RESULT_SET,
				m_resBinding);

		if ((fromIndex = (page - 1)
				* FacetEnvironment.DefaultValue.NUMBER_OF_RESULTS_PER_PAGE) > res
				.size()) {

			return null;

		} else {

			int toIndex = Math.min(page
					* FacetEnvironment.DefaultValue.NUMBER_OF_RESULTS_PER_PAGE,
					res.size());

			return res.subTable(fromIndex, toIndex);
		}
	}

	public HashSet<String> getSources(double nodeID) throws DatabaseException,
			IOException {

		return FacetDbUtils
				.getAllAsSet(m_subjectCache, FacetDbUtils
						.getKey(new String[] { String.valueOf(nodeID) }),
						m_subjBinding);
	}

	public boolean isOpen() {
		return (m_resCache != null) && (m_pathCache != null)
				&& (m_objectCache != null) && (m_subjectCache != null)
				&& (m_env != null);
	}

	@SuppressWarnings("unchecked")
	public void open() throws EnvironmentLockedException, DatabaseException {

		// init db
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		m_env = new Environment(m_dir, envConfig);

		// Databases without duplicates
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setDeferredWrite(true);

		// Databases with duplicates
		DatabaseConfig dbConfig2 = new DatabaseConfig();
		dbConfig2.setTransactional(false);
		dbConfig2.setAllowCreate(true);
		dbConfig2.setSortedDuplicates(true);
		dbConfig2.setDeferredWrite(true);

		m_resCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FRES_CACHE, dbConfig);

		m_pathCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FP_CACHE, dbConfig2);

		m_objectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO_CACHE, dbConfig2);

		m_subjectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS_CACHE, dbConfig2);

		// Create the bindings
		Database classDb = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.CLASS, dbConfig);

		StoredClassCatalog cata = new StoredClassCatalog(classDb);

		m_resBinding = new SerialBinding<Table>(cata, Table.class);

		// m_pathBinding = new SerialBinding<Node>(new StoredClassCatalog(
		// m_pathCache), Node.class);

		m_objBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_subjBinding = TupleBinding.getPrimitiveBinding(String.class);

		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(2000);

		m_resCache.preload(pc);
		m_pathCache.preload(pc);
		m_objectCache.preload(pc);
		m_subjectCache.preload(pc);

	}

	public void storeResultSet(Table<String> res)
			throws UnsupportedEncodingException {

		FacetDbUtils.store(m_resCache, Keys.RESULT_SET, res, m_resBinding);
	}
}
