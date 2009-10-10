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

import edu.unika.aifb.facetedSearch.facets.model.IFacetFacetValueTuple;

/**
 * @author andi
 * 
 */
public class FacetFacetValueTuple implements IFacetFacetValueTuple {

	private double m_id;
	private String m_domain;

	private Facet m_facet;
	private AbstractFacetValue m_abstractFacetValue;

	public FacetFacetValueTuple() {
	}

	public FacetFacetValueTuple(Facet facet, AbstractFacetValue value) {

		m_facet = facet;
		m_abstractFacetValue = value;
		m_domain = m_abstractFacetValue.getDomain();
		
		updateID();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof FacetFacetValueTuple) {

			return m_id == ((FacetFacetValueTuple) obj).getId();

		} else {
			return false;
		}
	}

	public String getDomain() {
		return m_domain;
	}

	public Facet getFacet() {
		return m_facet;
	}

	public AbstractFacetValue getFacetValue() {
		return m_abstractFacetValue;
	}

	public double getId() {
		return m_id;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setFacet(Facet facet) {

		m_facet = facet;
		m_domain = facet.getDomain();
		updateID();
	}

	public void setFacetValue(AbstractFacetValue abstractFacetValue) {

		m_abstractFacetValue = abstractFacetValue;
		m_domain = abstractFacetValue.getDomain();
		updateID();
	}

	@Deprecated
	public void setId(double id) {
		m_id = id;
	}

	private void updateID() {

		int domainHash = m_domain != null ? m_domain.hashCode() : 0;
		int facetHash = m_facet != null ? m_facet.hashCode() : 0;
		int facetValueHash = m_abstractFacetValue != null
				? m_abstractFacetValue.hashCode()
				: 0;

		m_id = domainHash + facetHash + facetValueHash;
	}
}
