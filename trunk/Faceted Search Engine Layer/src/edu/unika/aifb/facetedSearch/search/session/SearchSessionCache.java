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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

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
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 * 
 */
public class SearchSessionCache {

	public enum ClearType {
		ALL, PATHS, LITERALS, DISTANCES
	}

	private class Keys {
		public static final String RESULT_SET = "re";
		public static final String ROOT = "ro";
		public static final String RANGE_ROOT = "raro";
	}

	@SuppressWarnings("unused")
	private static final Logger s_log = Logger
			.getLogger(SearchSessionCache.class);

	private File m_dir;

	private Environment m_env;
	private DatabaseConfig m_dbConfig;
	private DatabaseConfig m_dbConfig2;

	private ArrayList<Database> m_dbs;

	/*
	 * dbs ...
	 */

	private Database m_resCache;
	private Database m_objectCache;
	private Database m_objectCountCache;
	private Database m_subjectCache;
	// private Database m_pathCache;
	private Database m_classDb;

	/*
	 * bindings
	 */

	@SuppressWarnings("unchecked")
	private EntryBinding<Table> m_resBinding;
	// @SuppressWarnings( { "unchecked", "unused" })
	// private EntryBinding<LinkedList> m_pathBinding;
	private EntryBinding<String> m_objBinding;
	private EntryBinding<String> m_subjBinding;

	/*
	 * other caches ...
	 */

	// private HashMap<Double, Queue<Edge>> m_paths2Root;
	// private HashMap<Double, Queue<Edge>> m_paths2RangeRoot;
	private HashMap<Integer, Object> m_parsedLiterals;
	private HashMap<Double, PriorityQueue<ClusterDistance>> m_clusterDistances;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		m_dir = dir;
		init();
	}

	public void addDistanceQueue(double id,
			PriorityQueue<ClusterDistance> distances) {

		m_clusterDistances.put(id, distances);
	}

	public int addObjects(HashSet<String> objects, Node node)
			throws UnsupportedEncodingException {

		int dups = 0;

		for (String object : objects) {

			try {
				FacetDbUtils.store(m_objectCache, String.valueOf(node), object,
						m_objBinding);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}

			try {
				FacetDbUtils.store(m_objectCountCache, String.valueOf(node)
						+ object, "", m_objBinding);
			} catch (DatabaseException e) {
				dups++;
			}
		}

		return objects.size() - dups;
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

	public void addParsedLiteral(String lit, Object parsedLit) {
		m_parsedLiterals.put(lit.hashCode(), parsedLit);
	}

	public void addSourceIndivdual(String ind, double nodeId)
			throws UnsupportedEncodingException {

		try {
			FacetDbUtils.store(m_subjectCache, FacetDbUtils
					.getKey(new String[] { String.valueOf(nodeId) }), ind,
					m_subjBinding);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void clear(ClearType type) throws DatabaseException {

		switch (type) {

		case ALL: {

			for (Database db : m_dbs) {

				if (db != null) {

					db.close();
					// m_env.removeDatabase(null, db.getDatabaseName());
				}
			}

			m_parsedLiterals.clear();

			// if (m_pathCache != null) {
			//
			// m_pathCache.close();
			// m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FP_CACHE);
			// }

			// m_paths2RangeRoot.clear();
			// m_paths2Root.clear();

			reOpen();
			break;
		}
		case LITERALS: {

			m_parsedLiterals.clear();
			break;
		}
		case DISTANCES: {

			m_clusterDistances.clear();
			break;
		}

			// case PATHS: {
			//
			// // m_paths2RangeRoot.clear();
			// // m_paths2Root.clear();
			//
			// // if (m_objectCountCache != null) {
			// //
			// // m_objectCountCache.close();
			// // m_env
			// // .removeDatabase(null,
			// // FacetDbUtils.DatabaseNames.FOC_CACHE);
			// // }
			// //
			// // if (m_objectCountCache != null) {
			// //
			// // m_objectCountCache.close();
			// // m_env
			// // .removeDatabase(null,
			// // FacetDbUtils.DatabaseNames.FOC_CACHE);
			// // }
			//			
			// break;
			// }
		default:
			break;
		}
	}

	public void close() throws DatabaseException {

		clear(ClearType.ALL);

		if (m_classDb != null) {
			m_classDb.close();
		}
		if (m_env != null) {
			m_env.close();
		}

		m_resCache = null;
		m_objectCache = null;
		m_subjectCache = null;
		m_objectCountCache = null;

		m_env = null;
	}

	// @SuppressWarnings("unchecked")
	public LinkedList<Edge> getAncestorPath2RangeRoot(FacetTree tree,
			double nodeId) throws DatabaseException, IOException {

		LinkedList<Edge> list;
		//
		// if ((list = FacetDbUtils.get(m_pathCache, nodeId + "rangeroot",
		// m_pathBinding)) == null) {

		list = tree.getAncestorPath2RangeRoot(nodeId);
		// FacetDbUtils.store(m_pathCache, nodeId + "rangeroot", list,
		// m_pathBinding);
		//
		// }

		return list;
	}

	// @SuppressWarnings("unchecked")
	public LinkedList<Edge> getAncestorPath2Root(FacetTree tree, double nodeId)
			throws DatabaseException, IOException {

		LinkedList<Edge> list;

		// if ((list = FacetDbUtils.get(m_pathCache, nodeId + "root",
		// m_pathBinding)) == null) {

		list = tree.getAncestorPath2Root(nodeId);
		// FacetDbUtils.store(m_pathCache, nodeId + "root", list,
		// m_pathBinding);
		//
		// }

		return list;
	}

	public PriorityQueue<ClusterDistance> getDistances(double id) {
		return m_clusterDistances.get(id);
	}

	public HashSet<String> getObjects(double nodeID) throws DatabaseException,
			IOException {

		return FacetDbUtils.getAllAsSet(m_objectCache, FacetDbUtils
				.getKey(new String[] { String.valueOf(nodeID) }), m_objBinding);
	}

	public Object getParsedLiteral(int litHash) {
		return m_parsedLiterals.get(litHash);
	}

	@SuppressWarnings("unchecked")
	public Table<String> getResults4Page(int page) throws DatabaseException,
			IOException {

		int fromIndex;
		Table<String> res = FacetDbUtils.get(m_resCache, Keys.RESULT_SET,
				m_resBinding);

		if ((fromIndex = (page - 1)
				* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE) > res
				.size()) {

			return null;

		} else {

			int toIndex = Math.min(page
					* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE,
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

	@SuppressWarnings("unchecked")
	private void init() throws EnvironmentLockedException, DatabaseException {

		// init stuff
		m_clusterDistances = new HashMap<Double, PriorityQueue<ClusterDistance>>();
		m_parsedLiterals = new HashMap<Integer, Object>();
		m_dbs = new ArrayList<Database>();

		// init db
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		m_env = new Environment(m_dir, envConfig);

		// Databases without duplicates
		m_dbConfig = new DatabaseConfig();
		m_dbConfig.setTransactional(false);
		m_dbConfig.setAllowCreate(true);
		m_dbConfig.setSortedDuplicates(false);
		// m_dbConfig.setDeferredWrite(true);
		m_dbConfig.setTemporary(true);

		// Databases with duplicates
		m_dbConfig2 = new DatabaseConfig();
		m_dbConfig2.setTransactional(false);
		m_dbConfig2.setAllowCreate(true);
		m_dbConfig2.setSortedDuplicates(true);
		// m_dbConfig2.setDeferredWrite(true);
		m_dbConfig2.setTemporary(true);

		// Databases without duplicates
		m_resCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FRES_CACHE, m_dbConfig);

		m_objectCountCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FOC_CACHE, m_dbConfig);

		// m_pathCache = m_env.openDatabase(null,
		// FacetDbUtils.DatabaseNames.FP_CACHE, m_dbConfig);

		// Databases with duplicates
		m_objectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO_CACHE, m_dbConfig2);

		m_subjectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS_CACHE, m_dbConfig2);

		m_dbs.add(m_resCache);
		m_dbs.add(m_objectCountCache);
		// m_dbs.add(m_pathCache);
		m_dbs.add(m_objectCache);
		m_dbs.add(m_subjectCache);

		// Create the bindings
		m_classDb = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.CLASS,
				m_dbConfig);

		StoredClassCatalog cata = new StoredClassCatalog(m_classDb);

		m_resBinding = new SerialBinding<Table>(cata, Table.class);
		// m_pathBinding = new SerialBinding<LinkedList>(cata,
		// LinkedList.class);
		m_objBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_subjBinding = TupleBinding.getPrimitiveBinding(String.class);

		// Preload dbs...
		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(FacetEnvironment.DefaultValue.PRELOAD_TIME);

		m_resCache.preload(pc);
		m_objectCountCache.preload(pc);
		m_objectCache.preload(pc);
		m_subjectCache.preload(pc);

	}

	public boolean isOpen() {

		boolean isOpen = true;

		for (Database db : m_dbs) {

			if (db == null) {
				isOpen = false;
				break;
			}
		}

		return isOpen;
	}

	public void reOpen() throws DatabaseException {

		// Databases without duplicates
		m_resCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FRES_CACHE, m_dbConfig);

		m_objectCountCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FOC_CACHE, m_dbConfig);

		// m_pathCache = m_env.openDatabase(null,
		// FacetDbUtils.DatabaseNames.FP_CACHE, m_dbConfig);

		// Databases with duplicates
		m_objectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO_CACHE, m_dbConfig2);

		m_subjectCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS_CACHE, m_dbConfig2);
	}

	public void storeResultSet(Table<String> res)
			throws UnsupportedEncodingException {

		try {
			FacetDbUtils.store(m_resCache, Keys.RESULT_SET, res, m_resBinding);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
}
