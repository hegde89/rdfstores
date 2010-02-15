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

/**
 * @author andi
 * 
 */
public class DateTimeLiteralFacetValueCluster extends LiteralFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8735183170646953125L;

	/*
	 * 
	 */
	private boolean m_hasCalChildren;

	/*
	 * 
	 */
	private int m_calClusterDepth;

	/*
	 * 
	 */
	private String m_calPrefix;

	public int getCalClusterDepth() {
		return m_calClusterDepth;
	}

	public String getCalPrefix() {
		return m_calPrefix;
	}

	public boolean hasCalChildren() {
		return m_hasCalChildren;
	}

	public void setCalClusterDepth(int calClusterDepth) {
		m_calClusterDepth = calClusterDepth;
	}

	public void setCalPrefix(String calPrefix) {
		m_calPrefix = calPrefix;
	}

	public void setHasCalChildren(boolean hasCalChildren) {
		m_hasCalChildren = hasCalChildren;
	}
}