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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.algo.ranking.RankingDelegator;
import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.ClearType;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;
import edu.unika.aifb.graphindex.index.IndexDirectory;

public class SearchSession {

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

	private int m_currentPage;
	private Map<String, Integer> m_currentLevels;

	/*
	 * properties
	 */
	private Properties m_props;

	private SearchSessionCache m_cache;
	private int m_id;

	protected SearchSession(GenericRdfStore store, int id, Properties props) {

		// init fields
		m_props = props;
		m_store = store;
		m_store.setSession(this);
		m_currentLevels = new HashMap<String, Integer>();
		m_facetTreeDelegator = FacetTreeDelegator.getInstance(this);
		m_rankingDelegator = RankingDelegator.getInstance(this);
		m_constructionDelegator = ConstructionDelegator.getInstance(this);
		m_id = id;

		initCache();
	}

	private void initCache() {

		// init cache
		try {

			m_cache = new SearchSessionCache(m_store.getIdxDir().getDirectory(
					IndexDirectory.FACET_SEARCH_LAYER_CACHE, true));

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
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

	public void clean() {

		try {
			m_cache.clear(ClearType.ALL);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		m_facetTreeDelegator.clear();
		m_rankingDelegator.clean();
		m_constructionDelegator.clean();

		System.gc();
	}

	public void close() {

		try {
			m_cache.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		m_facetTreeDelegator = null;
		m_rankingDelegator = null;
		m_constructionDelegator = null;

		System.gc();
	}

	/**
	 * @return the cache
	 */
	public SearchSessionCache getCache() {

		if (m_cache == null) {
			initCache();
		}

		return m_cache;
	}

	// /**
	// * @return the m_constructionDelegator
	// */
	// public ConstructionDelegator getConstructionDelegator() {
	// return this.m_constructionDelegator;
	// }

	/**
	 * @return the m_currentLevel
	 */
	public int getCurrentLevel(String extension) {
		return this.m_currentLevels.containsKey(extension)
				? this.m_currentLevels.get(extension)
				: -1;
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

	// /**
	// * @return the m_facetTreeDelegator
	// */
	// public FacetTreeDelegator getFacetTreeDelegator() {
	// return this.m_facetTreeDelegator;
	// }

	/**
	 * @return the id
	 */
	public int getId() {
		return this.m_id;
	}

	// /**
	// * @return the indFactory
	// */
	// public IndividualFactorty getIndFactory() {
	// return this.m_indFactory;
	// }
	//
	// /**
	// * @return the litFactory
	// */
	// public LiteralFactory getLitFactory() {
	// return this.m_litFactory;
	// }

	public Properties getProps() {
		return this.m_props;
	}

	public String getPropValue(Property key) {

		switch (key) {

			case ACTION : {
				return m_props.getProperty(FacetEnvironment.ACTION);
			}
			case CREATE_DATA_EXTENSIONS : {
				return m_props
						.getProperty(FacetEnvironment.CREATE_DATA_EXTENSIONS);
			}
			case CREATE_DATA_INDEX : {
				return m_props.getProperty(FacetEnvironment.CREATE_DATA_INDEX);
			}
			case CREATE_KEYWORD_INDEX : {
				return m_props
						.getProperty(FacetEnvironment.CREATE_KEYWORD_INDEX);
			}
			case CREATE_STRUCTURE_INDEX : {
				return m_props
						.getProperty(FacetEnvironment.CREATE_STRUCTURE_INDEX);
			}
			case FACETS_ENABLED : {
				return m_props.getProperty(FacetEnvironment.FACETS_ENABLED);
			}
			case FILES : {
				return m_props.getProperty(FacetEnvironment.FILES);
			}
			case IGNORE_DATATYPES : {
				return m_props.getProperty(FacetEnvironment.IGNORE_DATATYPES);
			}
			case INDEX_DIRECTORY : {
				return m_props.getProperty(FacetEnvironment.INDEX_DIRECTORY);
			}
			case NEIGHBORHOOD_SIZE : {
				return m_props.getProperty(FacetEnvironment.NEIGHBORHOOD_SIZE);
			}
			case ONTO_LANGUAGE : {
				return m_props.getProperty(FacetEnvironment.ONTO_LANGUAGE);
			}
			case RANKING_ENABLED : {
				return m_props.getProperty(FacetEnvironment.RANKING_ENABLED);
			}
			case STRUCTURE_BASED_DATA_PARTIONING : {
				return m_props
						.getProperty(FacetEnvironment.STRUCTURE_BASED_DATA_PARTIONING);
			}
			case STRUCTURE_INDEX_PATH_LENGTH : {
				return m_props
						.getProperty(FacetEnvironment.STRUCTURE_INDEX_PATH_LENGTH);
			}
			default :
				return null;
		}
	}

	// /**
	// * @return the m_rankingDelegator
	// */
	// public RankingDelegator getRankingDelegator() {
	// return this.m_rankingDelegator;
	// }

	/**
	 * @return the m_store
	 */
	public GenericRdfStore getStore() {
		return this.m_store;
	}

	/**
	 * @param currentPage the currentPage to set
	 */
	public void setCurrentPage(int currentPage) {
		m_currentPage = currentPage;
	}

	/**
	 * @return the currentPage
	 */
	public int getCurrentPage() {
		return m_currentPage;
	}

	// public boolean rankingEnabled() {
	// return Boolean.getBoolean(this.m_props
	// .getProperty(FacetEnvironment.RANKING_ENABLED));
	// }

	// /**
	// * @param level
	// * the m_currentLevel to set
	// */
	// public void setCurrentLevel(int level, String extension) {
	//
	// this.m_currentLevels.put(extension, level);
	// }
	//
	// /**
	// * @param id
	// * the id to set
	// */
	// public void setId(int id) {
	// this.m_id = id;
	// }
	//
	// /**
	// * @param indFactory
	// * the indFactory to set
	// */
	// public void setIndFactory(IndividualFactorty indFactory) {
	// this.m_indFactory = indFactory;
	// }
	//
	// /**
	// * @param litFactory
	// * the litFactory to set
	// */
	// public void setLitFactory(LiteralFactory litFactory) {
	// this.m_litFactory = litFactory;
	// }
}
