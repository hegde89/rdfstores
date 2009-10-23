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

	private static class IdxDir {

		private static final String TREE = "/tree";
		private static final String TEMP = "/temp";

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

	/*
	 * 
	 */
	private static int s_refinementMode = Integer.MIN_VALUE;

	/*
	 * 
	 */
	private static String s_graphIndexDirStrg;

	/*
	 * 
	 */
	private static String s_schemaOntoPath;

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
	private static int s_maxSearchSessions = Integer.MIN_VALUE;

	/*
	 * 
	 */

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
			s_cacheDir.mkdir();
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
			newCache.mkdir();

			s_cacheSessionDirs.put(key, newCache);
		}

		return s_cacheSessionDirs.get(key);
	}

	public static String getCacheDirStrg() {
		return s_cacheDirStrg;
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
			s_facetIdxTempDir.mkdir();
		}

		return s_facetIdxTreeDir;
	}

	public static File getFacetTreeIdxDir() {

		if (s_facetIdxTreeDir == null) {
			s_facetIdxTreeDir = new File(s_facetIdxDirStrg + IdxDir.TREE);
		}

		if (!s_facetIdxTreeDir.exists()) {
			s_facetIdxTreeDir.mkdir();
		}

		return s_facetIdxTreeDir;
	}

	public static String getGraphIndexDirStrg() {
		return s_graphIndexDirStrg;
	}

	public static int getMaxSearchSessions() {
		return s_maxSearchSessions == Integer.MIN_VALUE
				? FacetEnvironment.DefaultValue.MAX_SESSIONS
				: s_maxSearchSessions;
	}

	public static long getPreloadMaxBytes() {
		return s_preloadMaxBytes;
	}

	public static int getRefinementMode() {
		return s_refinementMode;
	}

	public static String getSchemaOntoPath() {
		return s_schemaOntoPath;
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
			facetIdxDir.mkdir();
		}
	}

	public static void setFacetsEnabled(boolean facetsEnabled) {
		s_facetsEnabled = facetsEnabled;
	}

	public static void setGraphIndexDirStrg(String graphIndexDirStrg) {
		s_graphIndexDirStrg = graphIndexDirStrg;
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

	public static void setRefinementMode(int refinementMode) {
		s_refinementMode = refinementMode;
	}

	public static void setSchemaOntoPath(String schemaOntoPath) {
		s_schemaOntoPath = schemaOntoPath;
	}
}