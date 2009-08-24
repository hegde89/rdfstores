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

import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.IFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.ILeave;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class FacetValue extends Node implements IFacetValue,ILeave {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4611143426289856264L;

	protected FacetValue(SearchSession session) {
		super(session);
	}

	private int m_countFV;
	private int m_countS;
	private String m_name;

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValue#getCountFV()
	 */
	public int getCountFV() {
		return m_countFV;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValue#getCountS()
	 */
	public int getCountS() {
		return m_countS;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValue#getName()
	 */
	public String getName() {
		return m_name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValue#setCountFV(int)
	 */
	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.api.IFacetValue#setCountS(int)
	 */
	public void setCountS(int countS) {
		m_countS = countS;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.api.IFacetValue#setName(java.lang
	 * .String)
	 */
	public void setName(String name) {
		m_name = name;
	}

	@Override
	public List<IFacetValueTuple> getChildren() {
		return null;
	}
	
	@Override
	public List<IFacetValueTuple> getChildren(boolean rankingEnabled) {
		return null;
	}
}
