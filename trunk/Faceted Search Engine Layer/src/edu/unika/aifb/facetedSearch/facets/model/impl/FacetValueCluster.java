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

import java.util.Set;

import edu.unika.aifb.facetedSearch.facets.model.IFacetValueCluster;

/**
 * @author andi
 * 
 */
public class FacetValueCluster extends AbstractFacetValue
		implements
			IFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = -549052164767648123L;

	/*
	 * 
	 */

	private String m_name;

	private Set<String> m_sourceExt;
	private Set<String> m_rangeExt;

	public String getName() {
		return m_name;
	}

	public Set<String> getRangeExt() {
		return m_rangeExt;
	}

	public Set<String> getSourceExt() {
		return m_sourceExt;
	}

	@Override
	public boolean isLeave() {
		return false;
	}

	public void setName(String name) {
		m_name = name;
	}

	public void setRangeExt(Set<String> rangeExt) {
		m_rangeExt = rangeExt;
	}

	public void setSourceExt(Set<String> sourceExt) {
		m_sourceExt = sourceExt;
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject#toString()
	 */
	@Override
	public String toString() {
		
		if(getName() != null) {
			return getName();
		} else {
			return super.toString();
		}
	}
}
