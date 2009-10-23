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

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCacheManager;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.algo.ranking.RankingDelegator;
import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.converter.facet2query.Facet2QueryModelConverter;
import edu.unika.aifb.facetedSearch.facets.converter.facet2tree.Facet2TreeModelConverter;
import edu.unika.aifb.facetedSearch.facets.converter.tree2facet.Tree2FacetModelConverter;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.fpage.FacetPageManager;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;

public class SearchSession {

	public enum CleanType {
		ALL
	}

	public enum Converters {
		FACET2TREE, TREE2FACET, FACET2QUERY
	}

	public enum DefaultValues {
		WEIGHT, DEPTH_K, NUMBER_OF_RESULTS_PER_PAGE, MAX_SESSIONS
	}

	public enum Delegators {
		TREE, RANKING, CONSTRUCTION
	}

	public enum Property {
		INDEX_DIRECTORY, RANKING_ENABLED, FACETS_ENABLED, ACTION, FILES, CREATE_DATA_INDEX, IGNORE_DATATYPES, CREATE_STRUCTURE_INDEX, CREATE_KEYWORD_INDEX, NEIGHBORHOOD_SIZE, STRUCTURE_INDEX_PATH_LENGTH, STRUCTURE_BASED_DATA_PARTIONING, CREATE_DATA_EXTENSIONS, ONTO_LANGUAGE
	}

	public enum SessionStatus {
		FREE, BUSY, CLOSED
	}

	/*
	 * store
	 */
	private GenericRdfStore m_store;

	/*
	 * Delegators
	 */
	private FacetTreeDelegator m_facetTreeDelegator;
	private RankingDelegator m_rankingDelegator;
	private ConstructionDelegator m_constructionDelegator;

	/*
	 * 
	 */
	private int m_currentPageNum;
	private FacetedQuery m_currentQuery;

	/*
	 * IDs
	 */
	private int m_searchSessionId;
	private String m_httpSessionId;

	/*
	 * 
	 */
	private SearchSessionCacheManager m_cacheManager;

	/*
	 * 
	 */
	private FacetPageManager m_fpageManager;

	/*
	 * 
	 */
	private SessionStatus m_sessionStatus;

	/*
	 * 
	 */
	private long m_timeStamp;

	protected SearchSession(GenericRdfStore store, int id) {

		m_searchSessionId = id;
		m_store = store;
		m_store.setSession(this);

		initCache();
		init();

		updateTimeStamp();
	}

	public void changeDefaultValue(DefaultValues name, int value) {

		updateTimeStamp();

		switch (name) {

			case DEPTH_K : {
				FacetEnvironment.DefaultValue.DEPTH_K = value;
				break;
			}
			case MAX_SESSIONS : {
				FacetEnvironment.DefaultValue.MAX_SESSIONS = value;
				break;
			}
			case NUMBER_OF_RESULTS_PER_PAGE : {
				FacetEnvironment.DefaultValue.NUM_OF_RESITEMS_PER_PAGE = value;
				break;
			}
			case WEIGHT : {
				FacetEnvironment.DefaultValue.WEIGHT = value;
				break;
			}
			default :
				break;
		}
	}

	public void clean() {
		clean(CleanType.ALL);
	}

	public void clean(CleanType type) {

		updateTimeStamp();

		switch (type) {

			case ALL : {

				try {

					m_cacheManager.get(m_searchSessionId).clean(
							SearchSessionCache.CleanType.ALL);

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				m_facetTreeDelegator.clean();
				m_rankingDelegator.clean();
				m_constructionDelegator.clean();
				m_fpageManager.clean();

				break;
			}
			default :
				break;
		}
	}

	public void close() {

		clean();

		try {
			m_cacheManager.get(m_searchSessionId).close();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (CacheException e) {
			e.printStackTrace();
		}

		m_facetTreeDelegator = null;
		m_rankingDelegator = null;
		m_constructionDelegator = null;

		setStatus(SessionStatus.CLOSED);
	}

	public SearchSessionCache getCache() {

		updateTimeStamp();
		return m_cacheManager.get(m_searchSessionId);
	}

	public AbstractConverter getConverter(Converters type) {

		updateTimeStamp();

		AbstractConverter converter;

		switch (type) {
			case FACET2TREE : {
				converter = Facet2TreeModelConverter.getInstance();
				break;
			}
			case TREE2FACET : {
				converter = new Tree2FacetModelConverter(this);
				break;
			}
			case FACET2QUERY : {
				converter = Facet2QueryModelConverter.getInstance();
				break;
			}
			default : {
				converter = null;
				break;
			}
		}

		return converter;
	}

	public int getCurrentPageNum() {

		updateTimeStamp();
		return m_currentPageNum;
	}

	public FacetedQuery getCurrentQuery() {

		updateTimeStamp();
		return m_currentQuery;
	}

	public Delegator getDelegator(Delegators name) {

		updateTimeStamp();

		switch (name) {

			case TREE :
				return m_facetTreeDelegator;

			case CONSTRUCTION :
				return m_constructionDelegator;

			case RANKING :
				return m_rankingDelegator;

			default :
				return Delegator.NULL;
		}
	}

	public FacetPageManager getFacetPageManager() {

		updateTimeStamp();
		return m_fpageManager;
	}

	public String getHttpSessionId() {
		return m_httpSessionId;
	}

	public int getSearchSessionId() {

		updateTimeStamp();
		return m_searchSessionId;
	}

	public GenericRdfStore getStore() {

		updateTimeStamp();
		return m_store;
	}

	protected long getTimeStamp() {
		return m_timeStamp;
	}

	public boolean hasHttpSessionId() {
		return m_httpSessionId != null;
	}

	private void init() {

		m_facetTreeDelegator = new FacetTreeDelegator(this);
		m_rankingDelegator = new RankingDelegator(this);
		m_constructionDelegator = new ConstructionDelegator(this);

		m_facetTreeDelegator.setConstructionDelegator(m_constructionDelegator);
		m_constructionDelegator.setTreeDelegator(m_facetTreeDelegator);
		m_cacheManager.get(m_searchSessionId).setTreeDelegator(
				m_facetTreeDelegator);

		m_fpageManager = new FacetPageManager(this);

		setStatus(SessionStatus.FREE);
	}

	private void initCache() {

		m_cacheManager = SearchSessionCacheManager.getInstance();

		try {

			Properties cacheProps = new Properties();
			cacheProps.load(new FileReader(FacetedSearchLayerConfig
					.getCacheConfigDirStrg()));
			cacheProps.put(FacetEnvironment.CacheConfig.DISK_PATH,
					FacetedSearchLayerConfig.getCacheDir() + "/"
							+ m_searchSessionId);

			CompositeCacheManager compositeCacheManager = CompositeCacheManager
					.getUnconfiguredInstance();
			compositeCacheManager.configure(cacheProps);

			SearchSessionCache cache = new SearchSessionCache(
					FacetedSearchLayerConfig
							.getCacheDir4Session(m_searchSessionId), this,
					compositeCacheManager);

			m_cacheManager.put(m_searchSessionId, cache);

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isClosed() {

		updateTimeStamp();
		return m_sessionStatus.equals(SessionStatus.CLOSED);
	}

	public boolean isFree() {

		updateTimeStamp();
		return m_sessionStatus.equals(SessionStatus.FREE);
	}

	public boolean isMySession(String httpSessionId) {

		return (m_httpSessionId != null)
				&& m_httpSessionId.equals(httpSessionId);
	}

	public void setCurrentPage(int currentPage) {

		updateTimeStamp();
		m_currentPageNum = currentPage;
	}

	public void setCurrentQuery(FacetedQuery currentQuery) {

		updateTimeStamp();
		m_currentQuery = currentQuery;
	}

	protected void setHttpSessionId(String httpSessionId) {
		m_httpSessionId = httpSessionId;
	}

	public void setStatus(SessionStatus sessionStatus) {

		updateTimeStamp();
		m_sessionStatus = sessionStatus;
	}

	public void touch() {
		updateTimeStamp();
	}

	protected void updateTimeStamp() {
		m_timeStamp = System.currentTimeMillis();
	}
}