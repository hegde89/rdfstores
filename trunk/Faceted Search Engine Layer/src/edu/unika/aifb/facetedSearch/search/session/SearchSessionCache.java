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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

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
	private Database m_edgeCache;
	private Database m_countFVCache;
	private Database m_countSCache;
	private Database m_object2subjectsCache;
	private Database m_distanceCache;
	private Database m_classDb;

	/*
	 * stored maps
	 */
	// private StoredMap<String, Integer> m_countFVMap;
	// private StoredMap<String, Integer> m_countSMap;
	private StoredMap<String, ClusterDistance> m_distanceMap;
	// private StoredMap<String, String> m_object2subjectsMap;

	/*
	 * bindings
	 */
	@SuppressWarnings("unchecked")
	private EntryBinding<Table> m_tableBinding;
	private EntryBinding<Edge> m_edgeBinding;
	private EntryBinding<ClusterDistance> m_distanceBinding;
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<Integer> m_intBinding;

	/*
	 * other caches ...
	 */

	private ArrayList<HashMap<? extends Object, ? extends Object>> m_maps;
	private HashMap<Integer, Object> m_parsedLiterals;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		m_dir = dir;
		init();
	}

	public Database getDB(String name) {

		if (name.equals(FacetDbUtils.DatabaseNames.FO_CACHE)) {

			return m_countFVCache;
			
		} else if (name.equals(FacetDbUtils.DatabaseNames.FS_CACHE)) {

			return m_countSCache;
			
		} else {

			return null;
		}
	}

	public void addDistance(String object1, String object2, String ext,
			ClusterDistance distance) throws UnsupportedEncodingException,
			DatabaseException {

		m_distanceMap.putIfAbsent(object1 + object2 + ext, distance);

		// FacetDbUtils.store(m_distanceCache, object1 + object2 + ext,
		// distance,
		// m_distanceBinding);
	}

	/**
	 * @return number of objects that were not present before, i.e. number of
	 *         non-duplicates
	 */
	public int addObjects(HashSet<String> objects, Node node, String source)
			throws DatabaseException, IOException {

		int dups = 0;

		for (String object : objects) {

			Integer countFV = m_countFVMap.get(String.valueOf(node.getID())
					+ object);

			if (countFV != null) {

				dups++;
				countFV = 0;
			}

			m_countFVMap
					.put(String.valueOf(node.getID()) + object, countFV + 1);

			addSource4Object(node, object, source);
		}

		return objects.size() - dups;
	}

	public void addParsedLiteral(String lit, Object parsedLit) {
		m_parsedLiterals.put(lit.hashCode(), parsedLit);
	}

	public void addSource4Node(String ind, Node node)
			throws UnsupportedEncodingException, DatabaseException {

		Integer countS = m_countSMap.get(String.valueOf(node.getID()));

		if (countS != null) {
			countS = 0;
		}

		m_countSMap.put(String.valueOf(node.getID()), countS + 1);

	}

	public void addSource4Object(Node node, String object, String source)
			throws UnsupportedEncodingException, DatabaseException {

		m_object2subjectsMap.put(String.valueOf(node.getID()) + object, source);

	}

	@SuppressWarnings("unchecked")
	public void clear(ClearType type) throws DatabaseException {

		switch (type) {

		case ALL: {

			for (Database db : m_dbs) {

				if (db != null) {
					db.close();
				}
			}

			for (HashMap map : m_maps) {

				if (map != null) {
					map.clear();
				}
			}

			reOpen();
			break;
		}
		case LITERALS: {

			m_parsedLiterals.clear();
			break;
		}
			// case DISTANCES: {
			//
			// m_clusterDistances.clear();
			// break;
			// }

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
	}

	public LinkedList<Edge> getAncestorPath2RangeRoot(FacetTree tree,
			double nodeId) throws DatabaseException, IOException {

		List<Edge> list;

		if ((list = FacetDbUtils.getAllAsList(m_edgeCache,
				nodeId + "rangeroot", m_edgeBinding)) == null) {

			list = tree.getAncestorPath2RangeRoot(nodeId);

			for (Edge edge : list) {
				FacetDbUtils.store(m_edgeCache, nodeId + "rangeroot", edge,
						m_edgeBinding);
			}
		}

		return new LinkedList<Edge>(list);
	}

	public LinkedList<Edge> getAncestorPath2Root(FacetTree tree, double nodeId)
			throws DatabaseException, IOException {

		List<Edge> list;

		if ((list = FacetDbUtils.getAllAsList(m_edgeCache, nodeId + "root",
				m_edgeBinding)) == null) {

			list = tree.getAncestorPath2RangeRoot(nodeId);

			for (Edge edge : list) {
				FacetDbUtils.store(m_edgeCache, nodeId + "root", edge,
						m_edgeBinding);
			}
		}

		return new LinkedList<Edge>(list);
	}

	public int getCountS4Object(Node node, String object)
			throws DatabaseException, IOException {

		return m_object2subjectsMap.duplicates(
				String.valueOf(node.getID()) + object).size();
	}

	public int getCountS4Objects(Node node, Collection<String> objects)
			throws DatabaseException, IOException {

		HashSet<String> allSources = new HashSet<String>();

		for (String object : objects) {
			allSources.addAll(m_object2subjectsMap.duplicates(String
					.valueOf(node.getID())
					+ object));
		}

		return allSources.size();
	}

	public int getCountS4Node(Node node) {
		return m_countSMap.get(String.valueOf(node.getID()));
	}

	public ClusterDistance getDistance(String object1, String object2,
			String ext) throws DatabaseException, IOException {

		return m_distanceMap.get(object1 + object2 + ext);
	}

	public HashSet<String> getObjects(Node node) throws DatabaseException,
			IOException {

		return m_countFVMap.;
	}

	public Object getParsedLiteral(int litHash) {
		return m_parsedLiterals.get(litHash);
	}

	@SuppressWarnings("unchecked")
	public Table<String> getResults4Page(int page) throws DatabaseException,
			IOException {

		int fromIndex;
		Table<String> res = FacetDbUtils.get(m_resCache, Keys.RESULT_SET,
				m_tableBinding);

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

	public HashSet<String> getSources4Node(double nodeID)
			throws DatabaseException, IOException {

		return FacetDbUtils.getAllAsSet(m_countSCache, String.valueOf(nodeID),
				m_strgBinding);
	}

	public HashSet<String> getSources4Object(String object)
			throws DatabaseException, IOException {

		return FacetDbUtils.getAllAsSet(m_object2subjectsCache, object,
				m_strgBinding);
	}

	@SuppressWarnings("unchecked")
	private void init() throws EnvironmentLockedException, DatabaseException {

		// init stuff
		m_parsedLiterals = new HashMap<Integer, Object>();

		m_maps = new ArrayList<HashMap<? extends Object, ? extends Object>>();
		m_maps.add(m_parsedLiterals);

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
		m_dbConfig.setTemporary(true);

		m_resCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FRES_CACHE, m_dbConfig);

		m_countFVCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO_CACHE, m_dbConfig);

		m_countSCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS_CACHE, m_dbConfig);

		m_distanceCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FD_CACHE, m_dbConfig);

		// Databases with duplicates
		m_dbConfig2 = new DatabaseConfig();
		m_dbConfig2.setTransactional(false);
		m_dbConfig2.setAllowCreate(true);
		m_dbConfig2.setSortedDuplicates(true);
		m_dbConfig2.setTemporary(true);

		m_object2subjectsCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO2S_CACHE, m_dbConfig2);

		m_edgeCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FE_CACHE, m_dbConfig2);

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_resCache);
		m_dbs.add(m_countFVCache);
		m_dbs.add(m_countSCache);
		m_dbs.add(m_edgeCache);
		m_dbs.add(m_object2subjectsCache);

		/*
		 * Create the bindings
		 */
		m_classDb = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.CLASS,
				m_dbConfig);

		StoredClassCatalog cata = new StoredClassCatalog(m_classDb);
		m_tableBinding = new SerialBinding<Table>(cata, Table.class);
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_intBinding = TupleBinding.getPrimitiveBinding(Integer.class);

		/*
		 * Create maps on top of dbs ...
		 */
		// m_countFVMap = new StoredMap<String, Integer>(m_countFVCache,
		// m_strgBinding, m_intBinding, true);
		// m_countSMap = new StoredMap<String, Integer>(m_countSCache,
		// m_strgBinding, m_intBinding, true);
		// m_object2subjectsMap = new StoredMap<String, String>(
		// m_object2subjectsCache, m_strgBinding, m_strgBinding, true);
		m_distanceMap = new StoredMap<String, ClusterDistance>(m_distanceCache,
				m_strgBinding, m_distanceBinding, true);

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

		m_countFVCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO_CACHE, m_dbConfig);

		m_countSCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS_CACHE, m_dbConfig);

		// Databases with duplicates
		m_object2subjectsCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FO2S_CACHE, m_dbConfig2);

		m_edgeCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FE_CACHE, m_dbConfig2);

		/*
		 * Create maps on top of dbs ...
		 */
		m_countFVMap = new StoredMap<String, Integer>(m_countFVCache,
				m_strgBinding, m_intBinding, true);
		m_countSMap = new StoredMap<String, Integer>(m_countSCache,
				m_strgBinding, m_intBinding, true);
		m_object2subjectsMap = new StoredMap<String, String>(
				m_object2subjectsCache, m_strgBinding, m_strgBinding, true);
		m_distanceMap = new StoredMap<String, ClusterDistance>(m_distanceCache,
				m_strgBinding, m_distanceBinding, true);

	}

	public void storeResultSet(Table<String> res)
			throws UnsupportedEncodingException {

		try {
			FacetDbUtils
					.store(m_resCache, Keys.RESULT_SET, res, m_tableBinding);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
}
