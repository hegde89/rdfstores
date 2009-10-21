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
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class SearchSessionFactory {

	private GenericRdfStore m_store;
	private RdfStoreConnection m_con;

	private static SearchSessionFactory s_instance;
	private static SearchSession s_pool[] = new SearchSession[FacetEnvironment.DefaultValue.MAX_SESSIONS];
	private static int[] s_locks = new int[FacetEnvironment.DefaultValue.MAX_SESSIONS];

	public static SearchSessionFactory getInstance(Properties props) {
		return s_instance == null
				? s_instance = new SearchSessionFactory(props)
				: s_instance;
	}

	private SearchSessionFactory(Properties props) {

		m_con = RdfStoreConnectionProvider.getInstance(props).getConnection();
		readProperties(props);

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

		for (int i = 0; i < FacetEnvironment.DefaultValue.MAX_SESSIONS; i++) {

			s_locks[i] = 0;

			try {
				s_pool[i] = new SearchSession(m_store, i, props);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public int acquire() throws InterruptedException {

		for (int i = 0; i < FacetEnvironment.DefaultValue.MAX_SESSIONS; i++) {
			if (s_locks[i] == 0) {
				s_locks[i] = 1;
				return i;
			}
		}

		System.err.println("acquire: All the semplore is being used!");
		return -1;
	}

	public void close() {
		m_con.close();
	}

	public void closeSession(SearchSession session) {
		session.clean(SearchSession.CleanType.ALL);
		// session.close();
		release(session.getId());
	}

	public SearchSession getSession(int id) {
		return s_pool[id];
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

		// FacetedSearchLayerConfig.setRefinementMode(Integer.parseInt((props
		// .getProperty(FacetEnvironment.Property.REFINEMENT_MODE))));

		FacetedSearchLayerConfig.setGraphIndexDirStrg((props
				.getProperty(FacetEnvironment.Property.GRAPH_INDEX_DIR)));

		FacetedSearchLayerConfig.setPreloadMaxBytes(Long.parseLong(props
				.getProperty(FacetEnvironment.Property.PRELOAD_MAX_BYTES)));

	}

	public void release(int id) {
		s_pool[id].clean(CleanType.ALL);
		s_locks[id] = 0;
	}
}
