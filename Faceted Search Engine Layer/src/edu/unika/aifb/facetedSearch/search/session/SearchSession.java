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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.unika.aifb.facetedSearch.Environment;
import edu.unika.aifb.facetedSearch.algo.construction.ConstructionDelegator;
import edu.unika.aifb.facetedSearch.algo.ranking.RankingDelegator;
import edu.unika.aifb.facetedSearch.api.model.impl.IndividualFactorty;
import edu.unika.aifb.facetedSearch.api.model.impl.LiteralFactory;
import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore;

public class SearchSession {

	private GenericRdfStore m_store;
	private FacetTreeDelegator m_facetTreeDelegator;
	private RankingDelegator m_rankingDelegator;
	private ConstructionDelegator m_constructionDelegator;
	private LiteralFactory m_litFactory;
	private IndividualFactorty m_indFactory;
	private Map<String, Integer> m_currentLevels;
	private Properties m_props;

	private int m_id;

	protected SearchSession(GenericRdfStore store, int id, Properties props) {

		this.m_props = props;
		this.m_store = store;
		this.m_store.setSession(this);
		this.m_currentLevels = new HashMap<String, Integer>();
		this.m_facetTreeDelegator = FacetTreeDelegator.getInstance(this);
		this.m_rankingDelegator = RankingDelegator.getInstance(this);
		this.m_constructionDelegator = ConstructionDelegator.getInstance(this);
		this.m_litFactory = new LiteralFactory(this);
		this.m_indFactory = new IndividualFactorty(this);
		this.setId(id);

	}

	public void changeDefaultLevel(int level) {
		Environment.DEFAULT_DEPTH_K = level;
	}

	public void clean() {
		this.m_facetTreeDelegator.clean();
		this.m_rankingDelegator.clean();
		this.m_constructionDelegator.clean();
	}

	/**
	 * @return the m_constructionDelegator
	 */
	public ConstructionDelegator getConstructionDelegator() {
		return this.m_constructionDelegator;
	}

	/**
	 * @return the m_currentLevel
	 */
	public int getCurrentLevel(String extension) {
		return this.m_currentLevels.containsKey(extension) ? this.m_currentLevels
				.get(extension)
				: -1;
	}

	/**
	 * @return the m_facetTreeDelegator
	 */
	public FacetTreeDelegator getFacetTreeDelegator() {
		return this.m_facetTreeDelegator;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return this.m_id;
	}

	/**
	 * @return the indFactory
	 */
	public IndividualFactorty getIndFactory() {
		return this.m_indFactory;
	}

	/**
	 * @return the litFactory
	 */
	public LiteralFactory getLitFactory() {
		return this.m_litFactory;
	}

	public Properties getProps() {
		return this.m_props;
	}

	/**
	 * @return the m_rankingDelegator
	 */
	public RankingDelegator getRankingDelegator() {
		return this.m_rankingDelegator;
	}

	/**
	 * @return the m_store
	 */
	public GenericRdfStore getStore() {
		return this.m_store;
	}

	public boolean rankingEnabled() {
		return Boolean.getBoolean(this.m_props
				.getProperty(Environment.RANKING_ENABLED));
	}

	/**
	 * @param level
	 *            the m_currentLevel to set
	 */
	public void setCurrentLevel(int level, String extension) {

		this.m_currentLevels.put(extension, level);
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(int id) {
		this.m_id = id;
	}

	/**
	 * @param indFactory
	 *            the indFactory to set
	 */
	public void setIndFactory(IndividualFactorty indFactory) {
		this.m_indFactory = indFactory;
	}

	/**
	 * @param litFactory
	 *            the litFactory to set
	 */
	public void setLitFactory(LiteralFactory litFactory) {
		this.m_litFactory = litFactory;
	}
}
