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
package edu.unika.aifb.facetedSearch.search.session.cache;

import it.unimi.dsi.fastutil.doubles.DoubleList;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.Keys;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicClusterNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.index.db.StorageHelperThread;
import edu.unika.aifb.facetedSearch.index.db.binding.LiteralListBinding;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class SearchSessionCache {

	public enum CleanType {
		ALL, NO_RES
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
	private CompositeCache m_objects4NodeCache;
	private CompositeCache m_subjects4NodeCache;
	private CompositeCache m_sources4NodeCache;
	private CompositeCache m_distanceCache;

	private CacheAccess m_objects4NodeCacheAccess;
	private CacheAccess m_subjects4NodeCacheAccess;
	private CacheAccess m_sources4NodeCacheAccess;
	private CacheAccess m_distanceCacheAccess;

	/*
	 * berkeley db configs/environment
	 */
	private String m_dirStrg;
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
	private Database m_litCache;
	private Database m_sourceCache;
	private Database m_leaveGroupsCache;

	/*
	 * delegator caches
	 */
	private Database m_fpageCache;
	private Database m_treeCache;

	/*
	 * history
	 */
	private Database m_historyCache;

	/*
	 * 
	 */
	private Database m_expansionCache;

	/*
	 * other
	 */
	private Database m_classDB;

	/*
	 * stored maps
	 */
	private StoredMap<String, Result> m_resMap;
	private StoredMap<Double, String> m_leave2subjectsMap;
	private StoredMap<String, String> m_object2sourceMap;
	private StoredMap<Double, List<AbstractSingleFacetValue>> m_dynNode2litListMap;

	/*
	 * bindings
	 */
	private EntryBinding<Result> m_resBinding;
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<Double> m_doubleBinding;
	private EntryBinding<List<AbstractSingleFacetValue>> m_litListBinding;

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * 
	 */
	private FacetIndex m_facetIdx;

	public SearchSessionCache(String dirStrg, SearchSession session,
			CompositeCacheManager compositeCacheManager)
			throws EnvironmentLockedException, DatabaseException {

		m_session = session;
		m_dirStrg = dirStrg;
		m_compositeCacheManager = compositeCacheManager;

		init();
	}

	public void addDistance(String object1, String object2,
			ClusterDistance distance) throws CacheException {

		m_distanceCacheAccess.put(object1 + object2, distance);
	}

	public void addObject2SourceMapping(String domain, String object,
			String source) {

		m_object2sourceMap.put(domain + object, source);
	}

	public void clean(CleanType type) throws DatabaseException, CacheException {

		switch (type) {

			case ALL : {

				if (m_resCache != null) {
					m_resMap.clear();
				}
				if (m_dynNode2litListMap != null) {
					m_dynNode2litListMap.clear();
				}
				if (m_object2sourceMap != null) {
					m_object2sourceMap.clear();
				}
				if (m_leave2subjectsMap != null) {
					m_leave2subjectsMap.clear();
				}
				if (m_sources4NodeCacheAccess != null) {
					m_sources4NodeCacheAccess.clear();
				}
				if (m_distanceCacheAccess != null) {
					m_distanceCacheAccess.clear();
				}
				if (m_subjects4NodeCacheAccess != null) {
					m_subjects4NodeCacheAccess.clear();
				}
				if (m_objects4NodeCacheAccess != null) {
					m_objects4NodeCacheAccess.clear();
				}

				break;
			}
			case NO_RES : {

				if (m_dynNode2litListMap != null) {
					m_dynNode2litListMap.clear();
				}
				if (m_object2sourceMap != null) {
					m_object2sourceMap.clear();
				}
				if (m_leave2subjectsMap != null) {
					m_leave2subjectsMap.clear();
				}
				if (m_sources4NodeCacheAccess != null) {
					m_sources4NodeCacheAccess.clear();
				}
				if (m_distanceCacheAccess != null) {
					m_distanceCacheAccess.clear();
				}
				if (m_subjects4NodeCacheAccess != null) {
					m_subjects4NodeCacheAccess.clear();
				}
				if (m_objects4NodeCacheAccess != null) {
					m_objects4NodeCacheAccess.clear();
				}

				break;
			}
		}
	}

	public void close() throws DatabaseException, CacheException {

		if (m_resCache != null) {
			m_resCache.close();
			m_resCache = null;
		}
		if (m_litCache != null) {

			m_litCache.close();
			m_litCache = null;
		}
		if (m_sourceCache != null) {

			m_sourceCache.close();
			m_sourceCache = null;
		}
		if (m_leaveGroupsCache != null) {

			m_leaveGroupsCache.close();
			m_leaveGroupsCache = null;
		}
		if (m_fpageCache != null) {

			m_fpageCache.close();
			m_fpageCache = null;
		}
		if (m_historyCache != null) {

			m_historyCache.close();
			m_historyCache = null;
		}
		if (m_expansionCache != null) {

			m_expansionCache.close();
			m_expansionCache = null;
		}
		if (m_sources4NodeCacheAccess != null) {
			m_sources4NodeCacheAccess.clear();
		}
		if (m_distanceCacheAccess != null) {
			m_distanceCacheAccess.clear();
		}
		if (m_subjects4NodeCacheAccess != null) {
			m_subjects4NodeCacheAccess.clear();
		}
		if (m_objects4NodeCacheAccess != null) {
			m_objects4NodeCacheAccess.clear();
		}
		if (m_classDB != null) {
			m_classDB.close();
		}
		if (m_env != null) {
			m_env.close();
		}

		m_leave2subjectsMap = null;
		m_object2sourceMap = null;
		m_dynNode2litListMap = null;
	}

	public int getCountFV(StaticClusterNode node) {

		if (node instanceof DynamicClusterNode) {

			return getCountFV4DynNode((DynamicClusterNode) node);

		} else if (node instanceof SingleValueNode) {

			return 1;

		} else {

			return getCountFV4StaticNode(node);
		}
	}

	public int getCountFV4DynNode(DynamicClusterNode dynamicClusterNode) {
		return dynamicClusterNode.getCountFV();
	}

	@Deprecated
	public int getCountFV4FacetValueNode(SingleValueNode singleValueNode) {
		return 1;
	}

	public int getCountFV4StaticNode(StaticClusterNode node) {

		if (!node.hasCountFV()) {
			int countFV = getObjects4StaticNode(node).size();
			node.setCountFV(countFV);
		}

		return node.getCountFV();
	}

	public int getCountS(StaticClusterNode node) {

		int countS;

		if (node instanceof DynamicClusterNode) {

			countS = getCountS4DynNode((DynamicClusterNode) node);

		} else if (node instanceof SingleValueNode) {

			countS = getCountS4FacetValueNode((SingleValueNode) node);

		} else {

			countS = getCountS4StaticNode(node);
		}

		return countS;
	}

	public int getCountS4DynNode(DynamicClusterNode dynamicClusterNode) {

		if (!dynamicClusterNode.hasCountS()) {
			int countS = getSources4DynNode(dynamicClusterNode).size();
			dynamicClusterNode.setCountS(countS);
		}

		return dynamicClusterNode.getCountS();
	}

	public int getCountS4FacetValueNode(SingleValueNode singleValueNode) {

		if (!singleValueNode.hasCountS()) {
			int countS = getSources4FacetValueNode(singleValueNode).size();
			singleValueNode.setCountS(countS);
		}

		return singleValueNode.getCountS();
	}

	public int getCountS4StaticNode(StaticClusterNode node) {

		int countS;

		if (!node.hasCountS()) {

			if (node.containsProperty()) {

				/*
				 * get range top & compute count/sources of range top ...
				 */
				StaticClusterNode rangeTop = (StaticClusterNode) m_treeDelegator
						.getRangeTop(node);
				countS = getSources4StaticNode(rangeTop).size();

				rangeTop.setCountS(countS);
				node.setCountS(countS);

			} else {

				countS = getSources4StaticNode(node).size();
				node.setCountS(countS);
			}
		}

		return node.getCountS();
	}

	public int getCountSOverlapping(StaticClusterNode node) {

		int countS;

		if (node instanceof DynamicClusterNode) {

			countS = getCountSOverlapping4DynNode((DynamicClusterNode) node);

		} else if (node instanceof SingleValueNode) {

			countS = getCountSOverlapping4FacetValueNode((SingleValueNode) node);

		} else {

			countS = getCountSOverlapping4StaticNode(node);
		}

		return countS;
	}

	public int getCountSOverlapping4DynNode(
			DynamicClusterNode dynamicClusterNode) {
		return dynamicClusterNode.getCountSOverlapping();
	}

	/**
	 * Use get getCountS4FacetValueNode() instead; returns the same value, since
	 * there is no overlapping for a single facet value!
	 */
	@Deprecated
	public int getCountSOverlapping4FacetValueNode(
			SingleValueNode singleValueNode) {

		if (!singleValueNode.hasCountS()) {
			int countS = getSources4FacetValueNode(singleValueNode).size();
			singleValueNode.setCountS(countS);
		}

		return singleValueNode.getCountS();
	}

	public int getCountSOverlapping4StaticNode(StaticClusterNode node) {

		int countSOverlapping = 0;

		if (!node.hasCountSOverlapping()) {

			if (node.containsProperty()) {

				/*
				 * get range top & compute count/sources of range top ...
				 */
				StaticClusterNode rangeTop = (StaticClusterNode) m_treeDelegator
						.getRangeTop(node);

				DoubleList leaveIDs = m_treeDelegator.getRangeLeaves(rangeTop
						.getDomain(), rangeTop.getID());

				for (double leaveID : leaveIDs) {
					countSOverlapping += m_treeDelegator.getNode(
							rangeTop.getDomain(), leaveID).getCountS();
				}

				rangeTop.setCountSOverlapping(countSOverlapping);
				node.setCountSOverlapping(countSOverlapping);

			} else {

				DoubleList leaveIDs = m_treeDelegator.getRangeLeaves(node
						.getDomain(), node.getID());

				for (double leaveID : leaveIDs) {
					countSOverlapping += m_treeDelegator.getNode(
							node.getDomain(), leaveID).getCountS();
				}

				node.setCountSOverlapping(countSOverlapping);
			}
		}

		return node.getCountSOverlapping();
	}

	public Result getCurrentResult() throws DatabaseException, IOException {
		return m_resMap.get(Keys.RESULT_SET_CURRENT);
	}

	public ResultPage getCurrentResultPage(int pageNum)
			throws DatabaseException, IOException {

		int fromIndex;
		Result res = FacetDbUtils.get(m_resCache, Keys.RESULT_SET_CURRENT,
				m_resBinding);

		Table<String> resTable = res.getResultTable();

		if ((fromIndex = (pageNum - 1)
				* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE) > resTable
				.rowCount()) {

			return ResultPage.EMPTY_PAGE;

		} else {

			int toIndex = Math.min(pageNum
					* FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE,
					resTable.rowCount());

			ResultPage resPage = new ResultPage();
			resPage.setPageNum(pageNum);
			resPage.setResultTable(res.getResultSubTable(fromIndex, toIndex));

			if (res.hasFacetPage()) {
				resPage.setFacetPage(res.getFacetPage());
			}

			return resPage;
		}
	}

	public Table<String> getCurrentResultTable() throws DatabaseException,
			IOException {

		return m_resMap.get(Keys.RESULT_SET_CURRENT).getResultTable();
	}

	public Database getDB(String name) {

		if (name.equals(FacetEnvironment.DatabaseName.CLASS)) {

			return m_classDB;

		} else if (name.equals(FacetEnvironment.DatabaseName.FPAGE_CACHE)) {

			return m_fpageCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FTREE_CACHE)) {

			return m_fpageCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FHIST_CACHE)) {

			return m_historyCache;

		} else if (name.equals(FacetEnvironment.DatabaseName.FEXP_CACHE)) {

			return m_expansionCache;

		} else {

			s_log.error("db with name '" + name + "' not specified!");
			return null;
		}
	}

	public ClusterDistance getDistance(String object1, String object2) {

		return (ClusterDistance) m_distanceCacheAccess.get(object1 + object2);
	}

	public List<AbstractSingleFacetValue> getLiterals4DynNode(
			DynamicClusterNode dynamicClusterNode) {

		return m_dynNode2litListMap.get(dynamicClusterNode.getID());
	}

	public List<AbstractSingleFacetValue> getLiterals4FacetValueNode(
			SingleValueNode singleValueNode) {

		return m_dynNode2litListMap.get(singleValueNode.getID());
	}

	@SuppressWarnings("unchecked")
	public Set<AbstractSingleFacetValue> getObjects4StaticNode(
			StaticClusterNode node) {

		HashSet<AbstractSingleFacetValue> objects;

		if ((objects = (HashSet<AbstractSingleFacetValue>) m_objects4NodeCacheAccess
				.get(node.getID())) == null) {

			objects = new HashSet<AbstractSingleFacetValue>();
			List<Double> leaveIDs = m_treeDelegator.getRangeLeaves(node
					.getDomain(), node.getID());

			for (double leaveID : leaveIDs) {

				Node leave = m_treeDelegator.getNode(node.getDomain(), leaveID);

				Collection<String> subjects = new HashSet<String>();
				subjects.addAll(getSubjects4Leave(leaveID));

				for (String subject : subjects) {

					try {

						Collection<AbstractSingleFacetValue> newObjects = m_facetIdx
								.getObjects(leave, subject);
						objects.addAll(newObjects);

					} catch (EnvironmentLockedException e) {
						e.printStackTrace();
					} catch (DatabaseException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			try {
				m_objects4NodeCacheAccess.put(node.getID(), objects);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return objects;
	}

	@SuppressWarnings("unchecked")
	public Set<AbstractSingleFacetValue> getObjects4StaticNode(
			StaticClusterNode node, String subject) {

		HashSet<AbstractSingleFacetValue> objects;

		if ((objects = (HashSet<AbstractSingleFacetValue>) m_objects4NodeCacheAccess
				.get(node.getID() + subject)) == null) {

			objects = new HashSet<AbstractSingleFacetValue>();
			List<Double> leaveIDs = m_treeDelegator.getRangeLeaves(node
					.getDomain(), node.getID());

			for (double leaveID : leaveIDs) {

				Node leave = m_treeDelegator.getNode(node.getDomain(), leaveID);

				try {

					Collection<AbstractSingleFacetValue> newObjects = m_facetIdx
							.getObjects(leave, subject);
					objects.addAll(newObjects);

				} catch (EnvironmentLockedException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			try {
				m_objects4NodeCacheAccess.put(node.getID() + subject, objects);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return objects;
	}

	@SuppressWarnings("unchecked")
	public Set<String> getSources4DynNode(DynamicClusterNode dynamicClusterNode) {

		HashSet<String> sources;

		if ((sources = (HashSet<String>) m_sources4NodeCacheAccess
				.get(dynamicClusterNode.getID())) == null) {

			sources = new HashSet<String>();
			List<AbstractSingleFacetValue> fvList = getLiterals4DynNode(dynamicClusterNode);

			for (AbstractSingleFacetValue fv : fvList) {

				Collection<String> newSources = new HashSet<String>(
						getSources4Object(dynamicClusterNode.getDomain(), fv
								.getValue()));

				sources.addAll(newSources);
			}

			try {
				m_sources4NodeCacheAccess.put(dynamicClusterNode.getID(),
						sources);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return sources;
	}

	public Set<String> getSources4FacetValueNode(SingleValueNode singleValueNode) {

		HashSet<String> sources = new HashSet<String>();
		sources.addAll(getSources4Object(singleValueNode.getDomain(),
				singleValueNode.getValue()));

		return sources;
	}

	@SuppressWarnings("unchecked")
	public Collection<String> getSources4Leave(String domain, double leaveID) {

		HashSet<String> sources;

		if ((sources = (HashSet<String>) m_sources4NodeCacheAccess.get(leaveID)) == null) {

			sources = new HashSet<String>();

			HashSet<String> subjects = new HashSet<String>();
			subjects.addAll(getSubjects4Leave(leaveID));

			for (String subject : subjects) {

				HashSet<String> newSources = new HashSet<String>();
				newSources.addAll(getSources4Object(domain, subject));

				if (!newSources.isEmpty()) {
					sources.addAll(newSources);
				} else {

					try {
						m_sources4NodeCacheAccess.put(leaveID, subjects);
					} catch (CacheException e) {
						e.printStackTrace();
					}

					return subjects;
				}
			}

			try {
				m_sources4NodeCacheAccess.put(leaveID, sources);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return sources;
	}

	public Set<String> getSources4Node(Node node) {

		Set<String> sources;

		if (node instanceof DynamicClusterNode) {

			sources = getSources4DynNode((DynamicClusterNode) node);

		} else if (node instanceof SingleValueNode) {

			sources = getSources4FacetValueNode((SingleValueNode) node);

		} else if (node instanceof StaticClusterNode) {

			sources = getSources4StaticNode((StaticClusterNode) node);

		} else {

			sources = new HashSet<String>();

		}

		return sources;
	}

	public Collection<String> getSources4Object(String domain, String object) {
		return m_object2sourceMap.duplicates(domain + object);
	}

	@SuppressWarnings("unchecked")
	public Set<String> getSources4StaticNode(StaticClusterNode node) {

		Set<String> sources;

		if ((sources = (Set<String>) m_sources4NodeCacheAccess
				.get(node.getID())) == null) {

			sources = new HashSet<String>();
			List<Double> leaveIDs = m_treeDelegator.getRangeLeaves(node
					.getDomain(), node.getID());

			for (double leaveID : leaveIDs) {

				Collection<String> newSources = getSources4Leave(node
						.getDomain(), leaveID);
				sources.addAll(newSources);
			}

			try {
				m_sources4NodeCacheAccess.put(node.getID(), sources);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return sources;
	}

	public Collection<String> getSubjects4Leave(double leaveID) {
		return m_leave2subjectsMap.duplicates(leaveID);
	}

	@SuppressWarnings("unchecked")
	public Collection<String> getSubjects4Node(Node node) {

		Collection<String> subjects;

		if ((subjects = (Collection<String>) m_subjects4NodeCacheAccess
				.get(node.getID())) == null) {

			subjects = new HashSet<String>();

			List<Double> leaveIDs = m_treeDelegator.getRangeLeaves(node
					.getDomain(), node.getID());

			for (double leaveID : leaveIDs) {

				Collection<String> newSubjects = new HashSet<String>(
						getSubjects4Leave(leaveID));
				subjects.addAll(newSubjects);
			}

			try {
				m_subjects4NodeCacheAccess.put(node.getID(), subjects);
			} catch (CacheException e) {
				e.printStackTrace();
			}
		}

		return subjects;
	}

	private void init() throws EnvironmentLockedException, DatabaseException {

		try {
			m_facetIdx = (FacetIndex) m_session.getStore().getIndex(
					IndexName.FACET_INDEX);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}

		/*
		 * JCS caches
		 */
		m_sources4NodeCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.SOURCES
						+ m_session.getSearchSessionId());
		m_distanceCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.DISTANCE
						+ m_session.getSearchSessionId());
		m_subjects4NodeCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.SUBJECTS
						+ m_session.getSearchSessionId());
		m_objects4NodeCache = m_compositeCacheManager
				.getCache(FacetEnvironment.CacheName.OBJECTS
						+ m_session.getSearchSessionId());

		m_sources4NodeCacheAccess = new CacheAccess(m_sources4NodeCache);
		m_distanceCacheAccess = new CacheAccess(m_distanceCache);
		m_subjects4NodeCacheAccess = new CacheAccess(m_subjects4NodeCache);
		m_objects4NodeCacheAccess = new CacheAccess(m_objects4NodeCache);

		/*
		 * Berkeley dbs ...
		 */

		File dirEnv1 = new File(m_dirStrg
				+ FacetEnvironment.DatabaseName.ENV_JDB1);
		dirEnv1.mkdir();

		// init db
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);
		m_env = new Environment(dirEnv1, envConfig);

		// Databases without duplicates
		m_dbConfig = new DatabaseConfig();
		m_dbConfig.setTransactional(false);
		m_dbConfig.setAllowCreate(true);
		m_dbConfig.setSortedDuplicates(false);
		m_dbConfig.setDeferredWrite(true);

		m_resCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FRES_CACHE, m_dbConfig);

		m_fpageCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FPAGE_CACHE, m_dbConfig);

		m_treeCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FTREE_CACHE, m_dbConfig);

		m_historyCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FHIST_CACHE, m_dbConfig);

		m_litCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FLIT_CACHE, m_dbConfig);

		m_expansionCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FEXP_CACHE, m_dbConfig);

		// databases with duplicates
		m_dbConfig2 = new DatabaseConfig();
		m_dbConfig2.setTransactional(false);
		m_dbConfig2.setAllowCreate(true);
		m_dbConfig2.setSortedDuplicates(true);
		m_dbConfig2.setDeferredWrite(true);

		m_sourceCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FS_CACHE, m_dbConfig2);

		m_leaveGroupsCache = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.FLG_CACHE, m_dbConfig2);

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_resCache);
		m_dbs.add(m_sourceCache);
		m_dbs.add(m_fpageCache);
		m_dbs.add(m_litCache);
		m_dbs.add(m_treeCache);
		m_dbs.add(m_historyCache);
		m_dbs.add(m_expansionCache);
		m_dbs.add(m_leaveGroupsCache);

		/*
		 * Create the bindings
		 */
		m_classDB = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.CLASS, m_dbConfig);

		m_resBinding = new SerialBinding<Result>(new StoredClassCatalog(
				m_classDB), Result.class);
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_doubleBinding = TupleBinding.getPrimitiveBinding(Double.class);
		m_litListBinding = new LiteralListBinding();

		/*
		 * Create maps on top of dbs ...
		 */
		m_resMap = new StoredMap<String, Result>(m_resCache, m_strgBinding,
				m_resBinding, true);

		m_leave2subjectsMap = new StoredMap<Double, String>(m_leaveGroupsCache,
				m_doubleBinding, m_strgBinding, true);

		m_object2sourceMap = new StoredMap<String, String>(m_sourceCache,
				m_strgBinding, m_strgBinding, true);

		m_dynNode2litListMap = new StoredMap<Double, List<AbstractSingleFacetValue>>(
				m_litCache, m_doubleBinding, m_litListBinding, true);
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

	public void setTreeDelegator(FacetTreeDelegator treeDelegator) {
		m_treeDelegator = treeDelegator;
	}

	public synchronized void storeCurrentResult(Result res)
			throws UnsupportedEncodingException, DatabaseException {

		StorageHelperThread<String, Result> storageHelper = new StorageHelperThread<String, Result>(
				m_resMap, Keys.RESULT_SET_CURRENT, res);
		storageHelper.start();
	}

	public void storeLiterals(DynamicClusterNode dynamicClusterNode,
			List<AbstractSingleFacetValue> lits) {
		m_dynNode2litListMap.put(dynamicClusterNode.getID(), lits);
	}

	public void updateLeaveGroups(double leaveID, String subject) {
		m_leave2subjectsMap.put(leaveID, subject);
	}
}