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

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.connection.impl.RdfStoreConnection;
import edu.unika.aifb.facetedSearch.connection.impl.RdfStoreConnectionProvider;
import edu.unika.aifb.facetedSearch.exception.MissingParameterException;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.CleanType;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.SessionStatus;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class SearchSessionFactory {

	protected static class SearchSessionFactoryUtils {

		protected static boolean isExpired(int sessionId) {

			return (System.currentTimeMillis() - s_pool[sessionId]
					.getTimeStamp()) > FacetedSearchLayerConfig
					.getMaxSearchSessionLength();
		}

	}
	/*
	 * 
	 */
	private static SearchSession s_pool[];
	private static int[] s_locks;

	/*
	 * 
	 */
	private static SearchSessionFactory s_instance;

	private static Thread s_sessionCleaningThread;

	public static SearchSessionFactory getInstance(Properties props) {
		return s_instance == null
				? s_instance = new SearchSessionFactory(props)
				: s_instance;
	}

	protected static int[] getLocks() {
		return s_locks;
	}

	protected static SearchSession[] getPool() {
		return s_pool;
	}

	/*
	 * 
	 */
	private GenericRdfStore m_store;

	private RdfStoreConnection m_con;

	private SearchSessionFactory(Properties props) {

		readProperties(props);
		init();

		m_con = RdfStoreConnectionProvider.getInstance(props).getConnection();

		try {

			m_store = m_con.loadOrCreateStore();

		} catch (InvalidParameterException e1) {
			e1.printStackTrace();
		} catch (MissingParameterException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (StorageException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		for (int i = 0; i < FacetedSearchLayerConfig.getMaxSearchSessions(); i++) {

			s_locks[i] = 0;

			try {
				s_pool[i] = new SearchSession(m_store, i);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		s_sessionCleaningThread = new SearchSessionCleaningThread();
		s_sessionCleaningThread.start();
	}

	public int acquire(String httpSessionId) throws InterruptedException {

		for (int i = 0; i < FacetedSearchLayerConfig.getMaxSearchSessions(); i++) {

			if (s_locks[i] == 0) {

				s_locks[i] = 1;
				s_pool[i].setHttpSessionId(httpSessionId);
				return i;
			}
		}

		for (int i = 0; i < FacetedSearchLayerConfig.getMaxSearchSessions(); i++) {

			if (SearchSessionFactoryUtils.isExpired(i)) {

				s_locks[i] = 1;
				s_pool[i].setHttpSessionId(httpSessionId);
				s_pool[i].touch();
				return i;
			}
		}

		System.err.println("acquire: all the fsl is being used!");
		return -1;
	}

	public void close() {

		m_con.close();
		releaseAndCloseAll();
		s_sessionCleaningThread.interrupt();
		
	}

	public SearchSession getSession(int id) {

		if (id != -1) {
			return s_pool[id];
		} else {
			return null;
		}
	}

	private void init() {
		s_pool = new SearchSession[FacetedSearchLayerConfig
				.getMaxSearchSessions()];
		s_locks = new int[FacetedSearchLayerConfig.getMaxSearchSessions()];
	}

	private void readProperties(Properties props) {

		FacetedSearchLayerConfig.setFacetIdxDirStrg(props
				.getProperty(FacetEnvironment.Property.FACET_INDEX_DIR));

		FacetedSearchLayerConfig.setFacetsEnabled(new Boolean(props
				.getProperty(FacetEnvironment.Property.FACETS_ENABLED)));

		FacetedSearchLayerConfig.setRankingEnabled(new Boolean(props
				.getProperty(FacetEnvironment.Property.RANKING_ENABLED)));

		FacetedSearchLayerConfig.setExpressivity(props
				.getProperty(FacetEnvironment.Property.EXPRESSIVITY));

		FacetedSearchLayerConfig.setCreateFacetIdx(new Boolean(props
				.getProperty(FacetEnvironment.Property.CREATE_FACET_IDX)));

		FacetedSearchLayerConfig.setCreateGraphIdx(new Boolean(props
				.getProperty(FacetEnvironment.Property.CREATE_GRAPH_IDX)));

		FacetedSearchLayerConfig.setCacheDirStrg((props
				.getProperty(FacetEnvironment.Property.CACHE_DIR)));

		FacetedSearchLayerConfig.setGraphIndexDirStrg((props
				.getProperty(FacetEnvironment.Property.GRAPH_INDEX_DIR)));

		FacetedSearchLayerConfig.setPreloadMaxBytes(Long.parseLong(props
				.getProperty(FacetEnvironment.Property.PRELOAD_MAX_BYTES)));

		FacetedSearchLayerConfig.setCacheConfigDirStrg(props
				.getProperty(FacetEnvironment.Property.CACHE_CONFIG));

		FacetedSearchLayerConfig.setMaxSearchSessions(Integer.parseInt(props
				.getProperty(FacetEnvironment.Property.MAX_SEARCH_SESSIONS)));

		FacetedSearchLayerConfig.setMaxSearchSessionLength(Long.parseLong(props
				.getProperty(FacetEnvironment.Property.MAX_SEARCH_LENGTH)));

		FacetedSearchLayerConfig.setCleaningInterval(Long.parseLong(props
				.getProperty(FacetEnvironment.Property.CLEANING_INTERVAL)));

	}

	public void release(int id) {

		if (id != -1) {

			s_pool[id].clean(CleanType.ALL);
			s_pool[id].setStatus(SessionStatus.FREE);
			s_pool[id].setHttpSessionId(null);
			s_locks[id] = 0;
		}
	}

	public void release(int id, String httpSessionId) {

		if (s_pool[id].getHttpSessionId() != null) {

			if (s_pool[id].getHttpSessionId().equals(httpSessionId)) {

				if (id != -1) {

					s_pool[id].clean(CleanType.ALL);
					s_pool[id].setStatus(SessionStatus.FREE);
					s_pool[id].setHttpSessionId(null);
					s_locks[id] = 0;
				}
			}
		} else {

			if (id != -1) {

				s_pool[id].clean(CleanType.ALL);
				s_pool[id].setStatus(SessionStatus.FREE);
				s_pool[id].setHttpSessionId(null);
				s_locks[id] = 0;
			}
		}
	}

	private void releaseAndCloseAll() {

		for (int i = 0; i < FacetedSearchLayerConfig.getMaxSearchSessions(); i++) {

			if (SearchSessionFactoryUtils.isExpired(i)) {

				s_pool[i].close();
				s_pool[i].setStatus(SessionStatus.FREE);
				s_pool[i].setHttpSessionId(null);
				s_locks[i] = 0;
			}
		}
	}
}