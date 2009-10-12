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
		ALL, REFINEMENT
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
	 * properties
	 */
	private Properties m_props;

	/*
	 * 
	 */
	private int m_id;
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private FacetPageManager m_fpageManager;

	protected SearchSession(GenericRdfStore store, int id, Properties props) {

		m_id = id;
		m_props = props;
		m_store = store;
		m_store.setSession(this);

		init();
	}

	public void changeDefaultValue(DefaultValues name, int value) {

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

	public void clean(CleanType type) {

		switch (type) {

			case ALL : {

				try {
					m_cache.clean(SearchSessionCache.CleanType.ALL);
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				m_facetTreeDelegator.clean();
				m_rankingDelegator.clean();
				m_constructionDelegator.clean();

				m_fpageManager.reOpen();

				System.gc();
				break;
			}
			case REFINEMENT : {

				try {
					m_cache.clean(SearchSessionCache.CleanType.REFINEMENT);
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (CacheException e) {
					e.printStackTrace();
				}

				m_facetTreeDelegator.clean();
				m_rankingDelegator.clean();
				m_constructionDelegator.clean();

				m_fpageManager.reOpen();

				System.gc();
				break;
			}
			default :
				break;
		}
	}

	public void close() {

		try {
			m_cache.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (CacheException e) {
			e.printStackTrace();
		}

		m_facetTreeDelegator = null;
		m_rankingDelegator = null;
		m_constructionDelegator = null;

		System.gc();
	}

	public SearchSessionCache getCache() {

		if (m_cache == null) {
			initCache();
		}

		return m_cache;
	}

	public AbstractConverter getConverter(Converters type) {

		AbstractConverter converter;

		switch (type) {
			case FACET2TREE : {
				converter = Facet2TreeModelConverter.getInstance(this);
				break;
			}
			case TREE2FACET : {
				converter = Tree2FacetModelConverter.getInstance(this);
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
		return m_currentPageNum;
	}

	public FacetedQuery getCurrentQuery() {
		return m_currentQuery;
	}

	public Delegator getDelegator(Delegators name) {

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
		return m_fpageManager;
	}

	public int getId() {
		return m_id;
	}

	public Properties getProps() {
		return this.m_props;
	}

	public String getPropValue(Property key) {

		switch (key) {

			case CREATE_DATA_EXTENSIONS : {
				return m_props
						.getProperty(FacetEnvironment.Property.CREATE_DATA_EXTENSIONS);
			}
			case CREATE_DATA_INDEX : {
				return m_props
						.getProperty(FacetEnvironment.Property.CREATE_DATA_INDEX);
			}
			case CREATE_KEYWORD_INDEX : {
				return m_props
						.getProperty(FacetEnvironment.Property.CREATE_KEYWORD_INDEX);
			}
			case CREATE_STRUCTURE_INDEX : {
				return m_props
						.getProperty(FacetEnvironment.Property.CREATE_STRUCTURE_INDEX);
			}
			case FACETS_ENABLED : {
				return m_props
						.getProperty(FacetEnvironment.Property.FACETS_ENABLED);
			}
			case FILES : {
				return m_props.getProperty(FacetEnvironment.Property.FILES);
			}
			case IGNORE_DATATYPES : {
				return m_props
						.getProperty(FacetEnvironment.Property.IGNORE_DATATYPES);
			}
			case INDEX_DIRECTORY : {
				return m_props
						.getProperty(FacetEnvironment.Property.GRAPH_INDEX_DIR);
			}
			case NEIGHBORHOOD_SIZE : {
				return m_props
						.getProperty(FacetEnvironment.Property.NEIGHBORHOOD_SIZE);
			}
			case ONTO_LANGUAGE : {
				return m_props
						.getProperty(FacetEnvironment.Property.ONTO_LANGUAGE);
			}
			case RANKING_ENABLED : {
				return m_props
						.getProperty(FacetEnvironment.Property.RANKING_ENABLED);
			}
			case STRUCTURE_BASED_DATA_PARTIONING : {
				return m_props
						.getProperty(FacetEnvironment.Property.STRUCTURE_BASED_DATA_PARTIONING);
			}
			case STRUCTURE_INDEX_PATH_LENGTH : {
				return m_props
						.getProperty(FacetEnvironment.Property.STRUCTURE_INDEX_PATH_LENGTH);
			}
			default :
				return null;
		}
	}

	public GenericRdfStore getStore() {
		return this.m_store;
	}

	private void init() {

		m_facetTreeDelegator = FacetTreeDelegator.getInstance(this);
		m_rankingDelegator = RankingDelegator.getInstance(this);
		m_constructionDelegator = ConstructionDelegator.getInstance(this);

		m_facetTreeDelegator.setConstructionDelegator(m_constructionDelegator);
		m_constructionDelegator.setTreeDelegator(m_facetTreeDelegator);

		m_fpageManager = FacetPageManager.getInstance(this);

		initCache();
	}

	private void initCache() {

		try {

			Properties cacheProps = new Properties();
			cacheProps.load(new FileReader(m_props
					.getProperty(FacetEnvironment.Property.CACHE_CONFIG)));
			cacheProps.put("jcs.auxiliary.DC.attributes.DiskPath",
					FacetedSearchLayerConfig.getCacheDir() + "/" + m_id);

			CompositeCacheManager compositeCacheManager = CompositeCacheManager
					.getUnconfiguredInstance();
			compositeCacheManager.configure(cacheProps);

			m_cache = new SearchSessionCache(FacetedSearchLayerConfig
					.getCacheDir4Session(m_id), this, compositeCacheManager);

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setCurrentPage(int currentPage) {
		m_currentPageNum = currentPage;
	}

	public void setCurrentQuery(FacetedQuery currentQuery) {
		m_currentQuery = currentQuery;
	}
}
