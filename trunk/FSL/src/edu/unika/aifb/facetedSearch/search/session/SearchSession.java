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
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.fpage.impl.FacetPageManager;
import edu.unika.aifb.facetedSearch.search.history.HistoryManager;
import edu.unika.aifb.facetedSearch.search.session.cache.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.cache.SearchSessionCacheManager;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;

public class SearchSession {

	public enum CleanType {
		ALL, REFINEMENT, EXPANSION
	}

	public enum Converters {
		FACET2TREE, TREE2FACET, FACET2QUERY
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

	private FacetPageManager m_fpageManager;
	private SearchSessionCacheManager m_cacheManager;
	private HistoryManager m_historyManager;

	/*
	 * 
	 */
	private CompositeCacheManager m_compositeCacheManager;

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

		touch();
	}

	public void clean() {
		clean(CleanType.ALL);
	}

	public void clean(CleanType type) {

		touch();

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
				m_historyManager.clean();

				m_currentQuery.clean();
				m_currentPageNum = 1;

				break;
			}
			case REFINEMENT : {

				try {

					m_cacheManager.get(m_searchSessionId).clean(
							SearchSessionCache.CleanType.ALL);

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				m_rankingDelegator.clean();
				m_constructionDelegator.clean();

				m_fpageManager.clean();
				m_currentPageNum = 1;

				break;
			}
			case EXPANSION : {

				try {

					m_cacheManager.get(m_searchSessionId).clean(
							SearchSessionCache.CleanType.NO_RES);

				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				m_facetTreeDelegator.clean();
				m_rankingDelegator.clean();
				m_constructionDelegator.clean();

				m_fpageManager.clean();
				m_currentQuery.clean();
				m_currentPageNum = 1;

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

		m_fpageManager = null;
		m_historyManager = null;

		setStatus(SessionStatus.CLOSED);
	}

	public SearchSessionCache getCache() {

		touch();
		return m_cacheManager.get(m_searchSessionId);
	}

	public CompositeCacheManager getCompositeCacheManager() {
		return m_compositeCacheManager;
	}

	public AbstractConverter getConverter(Converters type) {

		touch();

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
				converter = Facet2QueryModelConverter.getInstance(this);
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

		touch();
		return m_currentPageNum;
	}

	public FacetedQuery getCurrentQuery() {

		touch();
		return m_currentQuery;
	}

	public Delegator getDelegator(Delegators name) {

		touch();

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

		touch();
		return m_fpageManager;
	}

	public HistoryManager getHistoryManager() {

		touch();
		return m_historyManager;
	}

	public String getHttpSessionId() {

		touch();
		return m_httpSessionId;
	}

	public int getSearchSessionId() {

		touch();
		return m_searchSessionId;
	}

	public GenericRdfStore getStore() {

		touch();
		return m_store;
	}

	protected long getTimeStamp() {
		return m_timeStamp;
	}

	public boolean hasHttpSessionId() {
		return m_httpSessionId != null;
	}

	private void init() {

		/*
		 * delegator
		 */

		m_facetTreeDelegator = new FacetTreeDelegator(this);
		m_rankingDelegator = new RankingDelegator(this);
		m_constructionDelegator = new ConstructionDelegator(this);

		m_facetTreeDelegator.setConstructionDelegator(m_constructionDelegator);
		m_constructionDelegator.setTreeDelegator(m_facetTreeDelegator);
		m_cacheManager.get(m_searchSessionId).setTreeDelegator(
				m_facetTreeDelegator);

		/*
		 * other
		 */

		m_fpageManager = new FacetPageManager(this);
		m_historyManager = new HistoryManager(this);
		m_currentQuery = new FacetedQuery();

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

			m_compositeCacheManager = CompositeCacheManager
					.getUnconfiguredInstance();
			m_compositeCacheManager.configure(cacheProps);

			SearchSessionCache cache = new SearchSessionCache(
					FacetedSearchLayerConfig.getCacheDirStrg() + "/"
							+ m_searchSessionId, this, m_compositeCacheManager);

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

		touch();
		return m_sessionStatus.equals(SessionStatus.CLOSED);
	}

	public boolean isFree() {

		touch();
		return m_sessionStatus.equals(SessionStatus.FREE);
	}

	public void setCurrentPage(int currentPage) {

		touch();
		m_currentPageNum = currentPage;
	}

	public void setCurrentQuery(FacetedQuery currentQuery) {

		touch();
		m_currentQuery = currentQuery;
	}

	public void setHttpSessionId(String httpSessionId) {

		touch();
		m_httpSessionId = httpSessionId;
	}

	public void setStatus(SessionStatus sessionStatus) {

		touch();
		m_sessionStatus = sessionStatus;
	}

	public void touch() {
		m_timeStamp = System.currentTimeMillis();
	}
}