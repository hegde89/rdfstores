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

import edu.unika.aifb.facetedSearch.facets.model.IAbstractFacetValue;

/**
 * @author andi
 * 
 */
public abstract class AbstractFacetValue
		implements
			IAbstractFacetValue,
			Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2343480809304503452L;

	/*
	 * 
	 */
	private String m_domain;
	private double m_nodeId;
	private String m_value;

	public AbstractFacetValue() {

	}

	public AbstractFacetValue(String value) {
		m_value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof AbstractFacetValue) {

			return ((AbstractFacetValue) obj).getNodeId() == getNodeId();

		} else {
			return false;
		}
	}

	public String getDomain() {
		return m_domain;
	}

	public double getNodeId() {
		return m_nodeId;
	}

	public String getValue() {
		return m_value;
	}

	public abstract boolean isLeave();

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setNodeId(double nodeId) {
		m_nodeId = nodeId;
	}

	public void setValue(String value) {
		m_value = value;
	}

	@Override
	public String toString() {
		return m_value;
	}
}
