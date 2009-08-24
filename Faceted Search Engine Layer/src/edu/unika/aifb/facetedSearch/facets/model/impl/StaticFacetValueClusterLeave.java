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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.ILeave;
import edu.unika.aifb.facetedSearch.facets.model.IStaticFacetValueCluster;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class StaticFacetValueClusterLeave extends StaticFacetValueCluster
		implements ILeave, IStaticFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6609652615346720600L;
	private Map<IIndividual, Integer> m_sources;

	protected StaticFacetValueClusterLeave(SearchSession session) {
		super(session);
		this.m_sources = new HashMap<IIndividual, Integer>();
	}

	public void addSource(IIndividual ind) {

		Integer count;

		if (!this.m_sources.containsKey(ind)) {

			count = new Integer(1);
			this.m_sources.put(ind, count);
		} else {

			count = this.m_sources.get(ind);
			this.m_sources.put(ind, count++);
		}
	}

	@Override
	public List<IFacetValueTuple> getChildren() {
		return null;
	}

	@Override
	public List<IFacetValueTuple> getChildren(boolean rankingEnabled) {
		return null;
	}

	@Override
	public Map<IIndividual, Integer> getSources() {
		return this.m_sources;
	}
}
