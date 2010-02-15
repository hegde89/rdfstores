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
package edu.unika.aifb.facetedSearch;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author andi
 * 
 */
public class FacetedSearchLayerConfig {

	public static class DefaultValue {

		public static double HEIGHT_INDICATOR = 0.75;
		public static int MAX_SESSIONS = 10;
		public static long MAX_SESSION_LENGTH = 300000;
		public static String REFINEMENT_MODE = Value.Refinement.ONE_HOP;

	}

	private static class IdxDir {

		private static final String TREE = "/tree";
		private static final String TEMP = "/temp";

	}

	public static class Property {

		public static final String SCHEMA_ONTO_PATH = "schemaOntoPath";
		public static final String GRAPH_INDEX_DIR = "graphidx.dir";
		public static final String RANKING_ENABLED = "ranking.enabled";
		public static final String FACETS_ENABLED = "facets.enabled";
		public static final String FILES = "files";
		public static final String CREATE_DATA_INDEX = "createDataIndex";
		public static final String CREATE_KEYWORD_INDEX = "createKeywordIndex";
		public static final String IGNORE_DATATYPES = "ignoreDataTypes";
		public static final String CREATE_STRUCTURE_INDEX = "createStructureIndex";
		public static final String NEIGHBORHOOD_SIZE = "neighborhood.size";
		public static final String STRUCTURE_INDEX_PATH_LENGTH = "structureIndex.pathLength";
		public static final String STRUCTURE_BASED_DATA_PARTIONING = "structureBased.dataPartitioning";
		public static final String CREATE_DATA_EXTENSIONS = "createDataExtensions";
		public static final String ONTO_LANGUAGE = "onto.language";
		public static final String CACHE_CONFIG = "cache.config";
		public static final String EXPRESSIVITY = "expressivity";
		public static final String FACET_INDEX_DIR = "facetidx.dir";
		public static final String CREATE_GRAPH_IDX = "create.graphIdx";
		public static final String CREATE_FACET_IDX = "create.facetIdx";
		public static final String CACHE_DIR = "cache.dir";
		public static final String REFINEMENT_MODE = "refinementMode";
		public static final String PRELOAD_MAX_BYTES = "preloadBytes";
		public static final String MAX_SEARCH_SESSIONS = "maxSearchSessions";
		public static final String MAX_SEARCH_LENGTH = "maxSearchSessionLength";
		public static final String CLEANING_INTERVAL = "cleaningInterval";
		public static final String MAX_COMPUTATON_THREADS = "maxComputationThreads";
		public static final String RANKING_METRIC = "rankingMetric";
		public static final String CLUSTERER = "clusterer";

	}

	public static class Value {

		public static class LiteralClusterer {

			public static final String SIMPLE = "simple";
			public static final String SINGLE_LINKAGE = "singleLinkage";
		}

		public static class Ranking {

			public static final String COUNT_S = "countS";
			public static final String COUNT_FV = "countFV";
			public static final String BROWSE_ABILITY = "browse";
			public static final String MIN_EFFORT = "minEffort";
		}

		public static class Refinement {

			public static final String ONE_HOP = "one_hop";
			public static final String MORE_HOP = "more_hop";
		}

		public static final int INT_NOT_SET = -1;
		public static final String STRG_NOT_SET = "notset";
	}

	/*
	 * 
	 */
	private static String s_facetIdxDirStrg;
	private static File s_facetIdxTreeDir;
	private static File s_facetIdxTempDir;

	/*
	 * 
	 */
	private static boolean s_rankingEnabled;
	private static boolean s_facetsEnabled;

	/*
	 * 
	 */
	private static String s_expressivity;

	/*
	 * 
	 */
	private static boolean s_createGraphIdx;
	private static boolean s_createFacetIdx;

	/*
	 * 
	 */
	private static String s_cacheDirStrg;
	private static File s_cacheDir;
	private static Map<String, File> s_cacheSessionDirs;
	private static File s_sharedCacheDir;

	/*
	 * 
	 */
	private static String s_refinementMode;

	/*
	 * 
	 */
	private static String s_graphIndexDirStrg;

	/*
	 * 
	 */
	private static long s_preloadMaxBytes;

	/*
	 * 
	 */
	private static String s_cacheConfigDirStrg;

	/*
	 * 
	 */
	private static int s_maxSearchSessions;

	/*
	 * 
	 */
	private static long s_maxSearchSessionLength;

	/*
	 * 
	 */
	private static long s_cleaningInterval;

	/*
	 * 
	 */
	private static String s_rankingMetric;

	/*
	 * 
	 */
	private static String s_clusterer;

	static {

		s_maxSearchSessions = Value.INT_NOT_SET;
		s_maxSearchSessionLength = Value.INT_NOT_SET;
		s_refinementMode = Value.STRG_NOT_SET;
	}

	public static boolean createFacetIdx() {
		return s_createFacetIdx;
	}

	public static boolean createGraphIdx() {
		return s_createGraphIdx;
	}

	public static String getCacheConfigDirStrg() {
		return s_cacheConfigDirStrg;
	}

	public static File getCacheDir() {

		if (s_cacheDir == null) {
			s_cacheDir = new File(s_cacheDirStrg);
		}

		if (!s_cacheDir.exists()) {
			s_cacheDir.mkdirs();
		}

		return s_cacheDir;
	}

	public static File getCacheDir4Session(int sessionID) {

		String key = s_cacheDirStrg + "/" + sessionID;

		if (s_cacheSessionDirs == null) {
			s_cacheSessionDirs = new HashMap<String, File>();
		}

		if (!s_cacheSessionDirs.containsKey(key)) {

			File newCache = new File(s_cacheDirStrg + "/" + sessionID);
			newCache.mkdirs();

			s_cacheSessionDirs.put(key, newCache);
		}

		return s_cacheSessionDirs.get(key);
	}

	public static String getCacheDirStrg() {
		return s_cacheDirStrg;
	}

	public static long getCleaningInterval() {
		return s_cleaningInterval;
	}

	public static String getClusterer() {
		return s_clusterer;
	}

	public static String getExpressivity() {
		return s_expressivity;
	}

	public static String getFacetIndexDirStrg() {
		return s_facetIdxDirStrg;
	}

	public static File getFacetTempIdxDir() {

		if (s_facetIdxTempDir == null) {
			s_facetIdxTempDir = new File(s_facetIdxDirStrg + IdxDir.TEMP);
		}

		if (!s_facetIdxTempDir.exists()) {
			s_facetIdxTempDir.mkdirs();
		}

		return s_facetIdxTreeDir;
	}

	public static File getFacetTreeIdxDir() {

		if (s_facetIdxTreeDir == null) {
			s_facetIdxTreeDir = new File(s_facetIdxDirStrg + IdxDir.TREE);
		}

		if (!s_facetIdxTreeDir.exists()) {
			s_facetIdxTreeDir.mkdirs();
		}

		return s_facetIdxTreeDir;
	}

	public static String getGraphIndexDirStrg() {
		return s_graphIndexDirStrg;
	}

	public static long getMaxSearchSessionLength() {

		return s_maxSearchSessionLength == Value.INT_NOT_SET
				? DefaultValue.MAX_SESSION_LENGTH
				: s_maxSearchSessionLength;
	}

	public static int getMaxSearchSessions() {
		return s_maxSearchSessions == Value.INT_NOT_SET
				? DefaultValue.MAX_SESSIONS
				: s_maxSearchSessions;
	}

	public static long getPreloadMaxBytes() {
		return s_preloadMaxBytes;
	}

	public static String getRankingMetric() {
		return s_rankingMetric;
	}

	public static String getRefinementMode() {
		
		if(s_refinementMode.equals(Value.STRG_NOT_SET)) {
			return DefaultValue.REFINEMENT_MODE;
		}
		
		return s_refinementMode;
	}

	public static File getSharedCacheDir() {

		if (s_sharedCacheDir == null) {
			s_sharedCacheDir = new File(s_cacheDirStrg + "/"
					+ FacetEnvironment.DefaultValue.SHARED_CACHE_NAME);
		}

		if (!s_sharedCacheDir.exists()) {
			s_sharedCacheDir.mkdirs();
		}

		return s_sharedCacheDir;
	}

	public static boolean isFacetsEnabled() {
		return s_facetsEnabled;
	}

	public static boolean isRankingEnabled() {
		return s_rankingEnabled;
	}

	public static void setCacheConfigDirStrg(String cacheConfigDirStrg) {
		s_cacheConfigDirStrg = cacheConfigDirStrg;
	}

	public static void setCacheDir(File cacheDir) {
		s_cacheDir = cacheDir;
	}

	public static void setCacheDirStrg(String cacheDirStrg) {
		s_cacheDirStrg = cacheDirStrg;
	}

	public static void setCleaningInterval(long cleaningInterval) {
		s_cleaningInterval = cleaningInterval;
	}

	public static void setClusterer(String clusterer) {
		s_clusterer = clusterer;
	}

	public static void setCreateFacetIdx(boolean createFacetIdx) {
		s_createFacetIdx = createFacetIdx;
	}

	public static void setCreateGraphIdx(boolean createGraphIdx) {
		s_createGraphIdx = createGraphIdx;
	}

	public static void setExpressivity(String expressivity) {
		s_expressivity = expressivity;
	}

	public static void setFacetIdxDirStrg(String facetIdxDirStrg) {

		s_facetIdxDirStrg = facetIdxDirStrg;

		File facetIdxDir = new File(s_facetIdxDirStrg);
		if (!facetIdxDir.exists()) {
			facetIdxDir.mkdirs();
		}
	}

	public static void setFacetsEnabled(boolean facetsEnabled) {
		s_facetsEnabled = facetsEnabled;
	}

	public static void setGraphIndexDirStrg(String graphIndexDirStrg) {
		s_graphIndexDirStrg = graphIndexDirStrg;
	}

	public static void setMaxSearchSessionLength(long maxSearchSessionLength) {
		s_maxSearchSessionLength = maxSearchSessionLength;
	}

	public static void setMaxSearchSessions(int maxSearchSessions) {
		s_maxSearchSessions = maxSearchSessions;
	}

	public static void setPreloadMaxBytes(long preloadMaxBytes) {
		s_preloadMaxBytes = preloadMaxBytes;
	}

	public static void setRankingEnabled(boolean rankingEnabled) {
		s_rankingEnabled = rankingEnabled;
	}

	public static void setRankingMetric(String rankingMetric) {
		s_rankingMetric = rankingMetric;
	}

	public static void setRefinementMode(String refinementMode) {
		s_refinementMode = refinementMode;
	}

	@Deprecated
	public static void setSharedCacheDir(File sharedCacheDir) {
		s_sharedCacheDir = sharedCacheDir;
	}
}