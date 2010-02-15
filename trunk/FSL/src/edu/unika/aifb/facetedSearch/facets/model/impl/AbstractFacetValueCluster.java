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

import edu.unika.aifb.facetedSearch.facets.model.IFacetValueCluster;

/**
 * @author andi
 * 
 */
public abstract class AbstractFacetValueCluster extends AbstractFacetValue
		implements
			IFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = -549052164767648123L;

	/*
	 * 
	 */
	private boolean m_rangeRoot;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject
	 * #isLeave()
	 */
	@Override
	public boolean isLeave() {
		return false;
	}

	public boolean isRangeRoot() {
		return m_rangeRoot;
	}

	public void setIsRangeRoot(boolean isRangeRoot) {
		m_rangeRoot = isRangeRoot;
	}
}