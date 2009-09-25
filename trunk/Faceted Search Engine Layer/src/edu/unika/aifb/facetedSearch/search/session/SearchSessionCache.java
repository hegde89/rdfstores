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
import java.util.LinkedList;
import java.util.List;

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
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
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

	private File m_dir;

	/*
	 * db configs/environment
	 */
	private Environment m_env;
	private DatabaseConfig m_dbConfig;
	private DatabaseConfig m_dbConfig2;

	/*
	 * dbs ...
	 */
	private ArrayList<Database> m_dbs;

	private Database m_resCache;
	private Database m_edgeCache;
	private Database m_countFVCache;
	private Database m_countSCache;
	private Database m_sortedLitCache;
	private Database m_subjects4facetValueCache;
	private Database m_distanceCache;
	private Database m_treeCache;
	private Database m_historyCache;

	private Database m_classDB;

	/*
	 * stored maps
	 */
	private StoredMap<String, ClusterDistance> m_distanceMap;
	private StoredMap<String, String> m_subjects4facetValueMap;
	private StoredSortedMap<String, Literal> m_sortedLits4nodeMap;

	/*
	 * bindings
	 */
	private EntryBinding<Result> m_resBinding;
	private EntryBinding<Edge> m_edgeBinding;
	private EntryBinding<ClusterDistance> m_distanceBinding;
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<Literal> m_litBinding;

	public SearchSessionCache(File dir) throws EnvironmentLockedException,
			DatabaseException {

		m_dir = dir;
		init();
	}

	public void addDistance(String object1, String object2, String ext,
			ClusterDistance distance) throws UnsupportedEncodingException,
			DatabaseException {

		m_distanceMap.putIfAbsent(object1 + object2 + ext, distance);

	}

	public void addLiteral4Node(Node node, Literal lit)
			throws UnsupportedEncodingException, DatabaseException {

		m_sortedLits4nodeMap.put(String.valueOf(node.getID()), lit);
	}

	public void addSource4FacetValue(FacetValue fv, String source)
			throws UnsupportedEncodingException, DatabaseException {

		m_subjects4facetValueMap.put(fv.getExt() + fv.getValue(), source);
	}

	public void clean(CleanType type) throws DatabaseException {

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
				if (m_subjects4facetValueCache != null) {

					m_subjects4facetValueCache.close();
					m_subjects4facetValueCache = null;
				}
				if (m_edgeCache != null) {

					m_edgeCache.close();
					m_edgeCache = null;
				}

				/*
				 * maps
				 */
				m_distanceMap = null;
				m_sortedLits4nodeMap = null;
				m_subjects4facetValueMap = null;

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
		}
	}

	public void close() throws DatabaseException {

		clean(CleanType.ALL);

		if (m_classDB != null) {
			m_classDB.close();
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

		if (name.equals(FacetDbUtils.DatabaseNames.FO_CACHE)) {

			return m_countFVCache;

		} else if (name.equals(FacetDbUtils.DatabaseNames.FS_CACHE)) {

			return m_countSCache;

		} else if (name.equals(FacetDbUtils.DatabaseNames.FTREE_CACHE)) {

			return m_treeCache;

		} else if (name.equals(FacetDbUtils.DatabaseNames.CLASS)) {

			return m_classDB;

		} else if (name.equals(FacetDbUtils.DatabaseNames.FHIST_CACHE)) {

			return m_historyCache;

		} else {

			s_log.error("db with name '" + name + "' not specified!");
			return null;
		}
	}

	public ClusterDistance getDistance(String object1, String object2,
			String ext) throws DatabaseException, IOException {

		return m_distanceMap.get(object1 + object2 + ext);
	}

	public Collection<Literal> getLiterals4Node(Node node) {

		return m_sortedLits4nodeMap.duplicates(String.valueOf(node.getID()));
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

	public Collection<String> getSources4FacetValue(FacetValue fv)
			throws DatabaseException, IOException {

		return m_subjects4facetValueMap.duplicates(fv.getExt() + fv.getValue());
	}

	private void init() throws EnvironmentLockedException, DatabaseException {

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

		m_sortedLitCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FLIT_CACHE, m_dbConfig);

		m_treeCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FTREE_CACHE, m_dbConfig);

		m_historyCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FHIST_CACHE, m_dbConfig);

		// Databases with duplicates
		m_dbConfig2 = new DatabaseConfig();
		m_dbConfig2.setTransactional(false);
		m_dbConfig2.setAllowCreate(true);
		m_dbConfig2.setSortedDuplicates(true);
		m_dbConfig2.setTemporary(true);

		m_subjects4facetValueCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FS4FV_CACHE, m_dbConfig2);

		m_edgeCache = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FE_CACHE, m_dbConfig2);

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_resCache);
		m_dbs.add(m_countFVCache);
		m_dbs.add(m_countSCache);
		m_dbs.add(m_edgeCache);
		m_dbs.add(m_sortedLitCache);
		m_dbs.add(m_subjects4facetValueCache);
		m_dbs.add(m_treeCache);
		m_dbs.add(m_historyCache);

		/*
		 * Create the bindings
		 */
		m_classDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.CLASS,
				m_dbConfig);

		StoredClassCatalog cata = new StoredClassCatalog(m_classDB);

		m_resBinding = new SerialBinding<Result>(cata, Result.class);
		m_litBinding = new SerialBinding<Literal>(cata, Literal.class);
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

		/*
		 * Create maps on top of dbs ...
		 */
		m_distanceMap = new StoredMap<String, ClusterDistance>(m_distanceCache,
				m_strgBinding, m_distanceBinding, true);

		m_sortedLits4nodeMap = new StoredSortedMap<String, Literal>(
				m_sortedLitCache, m_strgBinding, m_litBinding, true);

		m_subjects4facetValueMap = new StoredMap<String, String>(
				m_subjects4facetValueCache, m_strgBinding, m_strgBinding, true);

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
					FacetDbUtils.DatabaseNames.FRES_CACHE, m_dbConfig);
		}
		if (m_countFVCache == null) {
			m_countFVCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FO_CACHE, m_dbConfig);
		}
		if (m_countSCache == null) {
			m_countSCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FS_CACHE, m_dbConfig);
		}
		if (m_sortedLitCache == null) {
			m_sortedLitCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FLIT_CACHE, m_dbConfig);
		}
		if (m_treeCache == null) {
			m_treeCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FTREE_CACHE, m_dbConfig);
		}
		if (m_historyCache == null) {
			m_historyCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FHIST_CACHE, m_dbConfig);
		}
		if (m_subjects4facetValueCache == null) {
			m_subjects4facetValueCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FS4FV_CACHE, m_dbConfig2);
		}
		if (m_edgeCache == null) {
			m_edgeCache = m_env.openDatabase(null,
					FacetDbUtils.DatabaseNames.FE_CACHE, m_dbConfig2);
		}

		/*
		 * Create maps on top of dbs ...
		 */

		if (m_distanceMap == null) {
			m_distanceMap = new StoredMap<String, ClusterDistance>(
					m_distanceCache, m_strgBinding, m_distanceBinding, true);
		}
		if (m_sortedLits4nodeMap == null) {
			m_sortedLits4nodeMap = new StoredSortedMap<String, Literal>(
					m_sortedLitCache, m_strgBinding, m_litBinding, true);
		}
		if (m_subjects4facetValueMap == null) {
			m_subjects4facetValueMap = new StoredMap<String, String>(
					m_subjects4facetValueCache, m_strgBinding, m_strgBinding,
					true);
		}
	}

	public void storeResult(Result res) throws UnsupportedEncodingException,
			DatabaseException {

		FacetDbUtils.store(m_resCache, Keys.RESULT_SET, res, m_resBinding);
	}
}
