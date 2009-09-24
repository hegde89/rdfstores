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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.IFacetFacetValueList;

/**
 * @author andi
 * 
 */
public class FacetFacetValueList implements IFacetFacetValueList {

	private Facet m_facet;
	private List<FacetValue> m_facetValueList;
	private List<FacetValue> m_facetValueHistory;
	
	public FacetFacetValueList() {

		/*
		 * init
		 */
		m_facetValueList = new ArrayList<FacetValue>();
		m_facetValueHistory = new ArrayList<FacetValue>();
		
	}

	public boolean addFacetValue2List(FacetValue fv) {
		return m_facetValueList.add(fv);
	}
	
	public boolean addFacetValue2History(FacetValue fv) {
		return m_facetValueHistory.add(fv);
	}

	public boolean listContains(FacetValue fv) {
		return m_facetValueList.contains(fv);
	}

	public Facet getFacet() {
		return m_facet;
	}

	public Iterator<FacetValue> getFacetValueListIterator() {
		return m_facetValueList.iterator();
	}

	public List<FacetValue> getFacetValueList() {
		return m_facetValueList;
	}

	public void setFacet(Facet facet) {
		m_facet = facet;
	}

	public void setFacetValueList(List<FacetValue> facetValueList) {
		m_facetValueList = facetValueList;
	}

	public void setFacetValueHistory(List<FacetValue> facetValueHistory) {
		m_facetValueHistory = facetValueHistory;
	}

	public List<FacetValue> getFacetValueHistory() {
		return m_facetValueHistory;
	}
}
