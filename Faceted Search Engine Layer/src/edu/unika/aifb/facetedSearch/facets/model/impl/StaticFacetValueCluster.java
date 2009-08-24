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
package edu.unika.aifb.facetedSearch.facets.model.impl;

import java.util.Map;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.facets.model.IStaticFacetValueCluster;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class StaticFacetValueCluster extends FacetValueCluster implements
		IStaticFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6015118963468592142L;

	private String m_luceneIdxTerm;

	protected StaticFacetValueCluster(SearchSession session) {
		super(session);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeedu.unika.aifb.facetedSearch.facets.api.IStaticFacetValueCluster#
	 * getLuceneIdxTerm()
	 */
	public String getLuceneIdxTerm() {
		return this.m_luceneIdxTerm;
	}

	@Override
	public Map<IIndividual, Integer> getSources() {
		// TODO GET SOURCES IMPORTANT
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeedu.unika.aifb.facetedSearch.facets.api.IStaticFacetValueCluster#
	 * setLuceneIdxTerm()
	 */
	public void setLuceneIdxTerm(String term) {
		this.m_luceneIdxTerm = term;
	}

}
