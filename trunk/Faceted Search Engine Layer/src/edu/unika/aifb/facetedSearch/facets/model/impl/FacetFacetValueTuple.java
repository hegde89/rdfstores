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

import java.io.Serializable;
import java.util.Random;

import edu.unika.aifb.facetedSearch.facets.model.IFacetFacetValueTuple;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class FacetFacetValueTuple
		implements
			IFacetFacetValueTuple,
			Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5049061404110005282L;

	/*
	 * 
	 */
	private double m_id;
	private String m_domain;

	/*
	 * 
	 */
	private Facet m_facet;
	private AbstractFacetValue m_abstractFacetValue;

	/*
	 * 
	 */

	public FacetFacetValueTuple() {
		init();
	}

	public FacetFacetValueTuple(Facet facet, AbstractFacetValue value) {

		m_facet = facet;
		m_abstractFacetValue = value;
		m_domain = m_abstractFacetValue.getDomain();
		init();
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

	private void init() {
		m_id = (new Random()).nextGaussian();
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setFacet(Facet facet) {

		m_facet = facet;

		if (m_domain == null) {
			m_domain = facet.getDomain();
		}
	}

	public void setFacetValue(AbstractFacetValue abstractFacetValue) {

		m_abstractFacetValue = abstractFacetValue;

		if (m_domain == null) {
			m_domain = abstractFacetValue.getDomain();
		}
	}

	@Deprecated
	public void setId(double id) {
		m_id = id;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Util.truncateUri(m_facet.getUri() + ": "
				+ Util.truncateUri(m_abstractFacetValue.getValue()));
	}
}