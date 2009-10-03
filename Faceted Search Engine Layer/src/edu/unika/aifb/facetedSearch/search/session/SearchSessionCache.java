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
import java.util.HashSet;

import org.apache.jcs.access.CacheAccess;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCache;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 * 
 */
public class SearchSessionCache {

	public enum BindingType {
		LITERAL
	}

	public enum CleanType {
		ALL, PATHS, LITERALS, DISTANCES, TREES, HISTORY
	}

	private class Keys {
		public static final String RESULT_SET = "re";
		public static final String ROOT = "ro";
		public static final String RANGE_ROOT = "raro";
	}

	private static final Logger s_log = Logger
			.getLogger(SearchSessionCache.class);

	/*
	 * JCS
	 */
	private CompositeCacheManager m_compositeCacheManager;

	/*
	 * Caches based on JCS
	 */

	private CompositeCache m_edgeCache;
	private CompositeCache m_distanceCache;
	private CacheAccess m_edgeCacheAccess;
	private CacheAccess m_distanceCacheAccess;

	/*
	 * berkeley db configs/environment
	 */
	private File m_dir;
	private Environment m_env;
	private DatabaseConfig m_dbConfig;
	private DatabaseConfig m_dbConfig2;

	/*
	 * Caches based on berkeley db ...
	 */
	private ArrayList<Database> m_dbs;

	/*
	 * fsl cache
	 */
	private Database m_resCache;
	private Database m_countFVCache;
	private Database m_countSCache;
	private Database m_sortedLitCache;
	private Database m_subjectsCache;

	/*
	 * tree delegator
	 */
	private Database m_treeCache;
	private Database m_fpageCache;
	/*
	 * history
	 */
	private Database m_historyCache;

	private Database m_classDB;

	/*
	 * stored maps
	 */
	private StoredMap<String, String> m_subjects4facetValueMap;
	// private StoredMap<Double, String> m_subjects4nodeMap;
	private StoredSortedMap<String, Literal> m_sortedLits4nodeMap;

	/*
	 * bindings
	 */
	private EntryBinding<Result> m_resBinding;
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<Literal> m_litBinding;
	// private EntryBinding<Double> m_doubleBinding;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		m_dir = dir;
		init();
	}

	public void addDistance(String object1, String object2, String ext,
			ClusterDistance distance) throws CacheException {

		m_distanceCacheAccess.put(object1 + object2 + ext, distance);

	}

	public void addLiteral4Node(Node node, Literal lit)
			throws UnsupportedEncodingException, DatabaseException {

		m_sortedLits4nodeMap.put(String.valueOf(node.getID()), lit);
	}

	public void addSource4FacetValue(AbstractSingleFacetValue fv, String source)
			throws UnsupportedEncodingException, DatabaseException {

		m_subjects4facetValueMap.put(fv.getSourceExt() + fv.getValue(), source);
	}

	// public void addSource4Node(double nodeID, String source)
	// throws UnsupportedEncodingException, DatabaseException {
	//
	// m_subjects4nodeMap.put(nodeID, source);
	// }

	public void clean(CleanType type) throws DatabaseException, CacheException {

		switch (type) {

			case ALL : {

				if (m_resCache != null) {

					m_resCache.close();
					m_resCache = null;
				}
				if (m_countFVCache != null) {

					m_countFVCache.close();
					m_countFVCache = null;
				}
				if (m_countSCache != null) {

					m_countSCache.close();
					m_countSCache = null;
				}
				if (m_sortedLitCache != null) {

					m_sortedLitCache.close();
					m_sortedLitCache = null;
				}
				if (m_treeCache != null) {

					m_treeCache.close();
					m_treeCache = null;
				}
				if (m_historyCache != null) {

					m_historyCache.close();
					m_historyCache = null;
				}
				if (m_subjectsCache != null) {

					m_subjectsCache.close();
					m_subjectsCache = null;
				}
				if (m_fpageCache != null) {

					m_fpageCache.close();
					m_fpageCache = null;
				}
				if (m_edgeCacheAccess != null) {
					m_edgeCacheAccess.clear();
				}
				if (m_distanceCacheAccess != null) {
					m_distanceCacheAccess.clear();
				}

				/*
				 * maps
				 */
				m_sortedLits4nodeMap = null;
				m_subjects4facetValueMap = null;
				// m_subjects4nodeMap = null;

				System.gc();
				reOpen();
				break;
			}
			case TREES : {

				if (m_treeCache != null) {

					m_treeCache.close();
					m_treeCache = null;
				}

				System.gc();
				reOpen();
				break;
			}
			case HISTORY : {

				if (m_historyCache != null) {

					m_historyCache.close();
					m_historyCache = null;
				}

				System.gc();
				reOpen();
				break;
			}
			case PATHS : {

				if (m_edgeCacheAccess != null) {
					m_edgeCacheAccess.clear();
				}

				System.gc();
				break;
			}
			case DISTANCES : {

				if (m_distanceCacheAccess != null) {
					m_distanceCacheAccess.clear();
				}

				System.gc();
				break;
			}
		}
	}

	public void close() throws DatabaseException, CacheException {

		clean(CleanType.ALL);

		if (m_classDB != null) {
			m_classDB.close();
		}
		if (m_env != null) {
			m_env.close();
		}
	}

	// @SuppressWarnings("unchecked")
	// public LinkedList<Edge> getAncestorPath2RangeRoot(FacetTree tree,
	// double nodeID) throws CacheException {
	//
	// LinkedList<Edge> path;
	//
	// if ((path = (LinkedList<Edge>) m_edgeCacheAccess.get(String
	// .valueOf(nodeID)
	// + Keys.RANGE_ROOT)) == null) {
	//
	// path = tree.getAncestorPath2RangeRoot(nodeID);
	// m_edgeCacheAccess.put(String.valueOf(nodeID) + Keys.RANGE_ROOT,
	// path);
	//
	// }
	//
	// return path;
	// }

	// @SuppressWarnings("unchecked")
	// public LinkedList<Edge> getAncestorPath2Root(FacetTree tree, double
	// nodeID)
	// throws CacheException {
	//
	// LinkedList<Edge> path;
	//
	// if ((path = (LinkedList<Edge>) m_edgeCacheAccess.get(String
	// .valueOf(nodeID)
	// + Keys.ROOT)) == null) {
	//
	// path = tree.getAncestorPath2Root(nodeID);
	// m_edgeCacheAccess.put(String.valueOf(nodeID) + Keys.ROOT, path);
	//
	// }
	//
	// return path;
	// }

	public int getCountS4Object(String ext, String object)
			throws DatabaseException, IOException {

		return m_subjects4facetValueMap.duplicates(ext + object).size();
	}

	public int getCountS4Objects(String ext, Collection<String> objects)
			throws DatabaseException, IOException {

		HashSet<String> allSources = new HashSet<String>();

		for (String object : objects) {
			allSources
					.addAll(m_subjects4facetValueMap.duplicates(ext + object));
		}

		return allSources.size();
	}

	public Database getDB(String name) {

		if (name.equals(FacetEnvironment.DatabaseName.FCO_CACHE)) {

			return m_countFVCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FCS_CACHE)) {

			return m_countSCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FTREE_CACHE)) {

			return m_treeCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.CLASS)) {

			return m_classDB;

		} else if (name.equals(FacetEnvironment.DatabaseName.FHIST_CACHE)) {

			return m_historyCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FPAGE_CACHE)) {

			return m_fpageCache;

		} else {

			s_log.error("db with name '" + name + "' not specified!");
			return null;
		}
	}

	public ClusterDistance getDistance(String object1, String object2,
			String ext) {

		// return m_distanceMap.get(object1 + object2 + ext);
		return (ClusterDistance) m_distanceCacheAccess.get(object1 + object2
				+ ext);
	}

	public Collection<Literal> getLiterals4Node(Node node) {

		return m_sortedLits4nodeMap.duplicates(String.valueOf(node.getID()));
	}

	public Result getResult() throws DatabaseException, IOException {

		return FacetDbUtils.get(m_resCache, Keys.RESULT_SET, m_resBinding);
	}

	public ResultPage getResultPage(int pageNum) throws DatabaseException,
			IOException {

		int fromIndex;
		Result res = FacetDbUtils
				.get(m_resCache, Keys.RESULT_SET, m_resBinding);

		Table<String> resTable = res.getResultTable();

		if ((fromIndex = (pageNum - 1)
				* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE) > resTable
				.size()) {

			return ResultPage.EMPTY_PAGE;

		} else {

			int toIndex = Math.min(pageNum
					* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE,
					resTable.size());

			ResultPage resPage = new ResultPage();
			resPage.setPageNum(pageNum);
			resPage.setResultTable(res.getResultSubTable(fromIndex, toIndex));

			if (res.hasFacetPage()) {
				resPage.setFacetPage(res.getFacetPage());
			}

			return resPage;
		}
	}

	public Table<String> getResultTable() throws DatabaseException, IOException {

		return FacetDbUtils.get(m_resCache, Keys.RESULT_SET, m_resBinding)
				.getResultTable();
	}

	public Collection<String> getSources4FacetValue(AbstractSingleFacetValue fv)
			throws DatabaseException, IOException {

		return m_subjects4facetValueMap.duplicates(fv.getSourceExt()
				+ fv.getValue());
	}

	private void init() throws EnvironmentLockedException, DatabaseException {

		/*
		 * JCS caches
		 */
		m_edgeCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.EDGE);
		m_distanceCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.DISTANCE);

		m_edgeCacheAccess = new CacheAccess(m_edgeCache);
		m_distanceCacheAccess = new CacheAccess(m_distanceCache);

		/*
		 * Berkeley dbs ...
		 */

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
				FacetEnvironment.DatabaseName.FRES_CACHE, m_dbConfig);

		m_countFVCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FCO_CACHE, m_dbConfig);

		m_countSCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FCS_CACHE, m_dbConfig);

		m_sortedLitCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FLIT_CACHE, m_dbConfig);

		m_treeCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FTREE_CACHE, m_dbConfig);

		m_historyCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FHIST_CACHE, m_dbConfig);

		m_fpageCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FPAGE_CACHE, m_dbConfig);

		// Databases with duplicates
		m_dbConfig2 = new DatabaseConfig();
		m_dbConfig2.setTransactional(false);
		m_dbConfig2.setAllowCreate(true);
		m_dbConfig2.setSortedDuplicates(true);
		m_dbConfig2.setTemporary(true);

		m_subjectsCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FS_CACHE, m_dbConfig2);

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_resCache);
		m_dbs.add(m_countFVCache);
		m_dbs.add(m_countSCache);
		m_dbs.add(m_sortedLitCache);
		m_dbs.add(m_subjectsCache);
		m_dbs.add(m_treeCache);
		m_dbs.add(m_historyCache);
		m_dbs.add(m_fpageCache);

		/*
		 * Create the bindings
		 */
		m_classDB = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.CLASS, m_dbConfig);

		StoredClassCatalog cata = new StoredClassCatalog(m_classDB);

		m_resBinding = new SerialBinding<Result>(cata, Result.class);
		m_litBinding = new SerialBinding<Literal>(cata, Literal.class);
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

		/*
		 * Create maps on top of dbs ...
		 */

		m_sortedLits4nodeMap = new StoredSortedMap<String, Literal>(
				m_sortedLitCache, m_strgBinding, m_litBinding, true);

		m_subjects4facetValueMap = new StoredMap<String, String>(
				m_subjectsCache, m_strgBinding, m_strgBinding, true);

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

		/*
		 * databases
		 */

		if (m_resCache == null) {
			m_resCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FRES_CACHE, m_dbConfig);
		}
		if (m_countFVCache == null) {
			m_countFVCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FCO_CACHE, m_dbConfig);
		}
		if (m_countSCache == null) {
			m_countSCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FCS_CACHE, m_dbConfig);
		}
		if (m_sortedLitCache == null) {
			m_sortedLitCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FLIT_CACHE, m_dbConfig);
		}
		if (m_treeCache == null) {
			m_treeCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FTREE_CACHE, m_dbConfig);
		}
		if (m_historyCache == null) {
			m_historyCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FHIST_CACHE, m_dbConfig);
		}
		if (m_fpageCache == null) {
			m_fpageCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FPAGE_CACHE, m_dbConfig);
		}
		if (m_subjectsCache == null) {
			m_subjectsCache = m_env.openDatabase(null,
					FacetEnvironment.DatabaseName.FS_CACHE, m_dbConfig2);
		}

		/*
		 * Create maps on top of dbs ...
		 */

		if (m_sortedLits4nodeMap == null) {
			m_sortedLits4nodeMap = new StoredSortedMap<String, Literal>(
					m_sortedLitCache, m_strgBinding, m_litBinding, true);
		}
		if (m_subjects4facetValueMap == null) {
			m_subjects4facetValueMap = new StoredMap<String, String>(
					m_subjectsCache, m_strgBinding, m_strgBinding, true);
		}
		// if (m_subjects4nodeMap == null) {
		// m_subjects4nodeMap = new StoredMap<Double, String>(m_subjectsCache,
		// m_doubleBinding, m_strgBinding, true);
		// }
	}

	public void setCompositeCacheManager(
			CompositeCacheManager compositeCacheManager) {
		m_compositeCacheManager = compositeCacheManager;
	}

	public void storeResult(Result res) throws UnsupportedEncodingException,
			DatabaseException {

		FacetDbUtils.store(m_resCache, Keys.RESULT_SET, res, m_resBinding);
	}
}
