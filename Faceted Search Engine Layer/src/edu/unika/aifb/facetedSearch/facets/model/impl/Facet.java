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

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.facets.model.IFacet;

/**
 * @author andi
 * 
 */
public class Facet implements IFacet {

	private String m_domain;
	private double m_nodeId;

	private String m_uri;
	private FacetType m_facetType;
	private DataType m_dataType;

	public Facet(String uri) {
		m_uri = uri;
	}

	public Facet(String uri, FacetType ftype, DataType dtype) {
		m_uri = uri;
		m_facetType = ftype;
		m_dataType = dtype;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Facet) {

			return ((Facet) obj).getNodeId() == getNodeId();

		} else {
			return false;
		}
	}

	public DataType getDataType() {
		return m_dataType;
	}

	public String getDomain() {
		return m_domain;
	}

	public double getNodeId() {
		return m_nodeId;
	}

	public FacetType getType() {
		return m_facetType;
	}

	public String getUri() {
		return m_uri;
	}

	public boolean isDataPropertyBased() {
		return m_facetType == FacetType.DATAPROPERTY_BASED;
	}

	public boolean isObjectPropertyBased() {
		return m_facetType == FacetType.OBJECT_PROPERTY_BASED;
	}

	public void setDataType(DataType dataType) {
		m_dataType = dataType;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setNodeId(double nodeId) {
		m_nodeId = nodeId;
	}

	public void setType(FacetType type) {
		m_facetType = type;
	}

	public void setUri(String uri) {
		m_uri = uri;
	}

	@Override
	public String toString() {
		return m_uri;
	}
}
