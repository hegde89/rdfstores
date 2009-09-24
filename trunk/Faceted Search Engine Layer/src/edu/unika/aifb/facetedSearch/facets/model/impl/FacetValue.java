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

import edu.unika.aifb.facetedSearch.facets.model.IFacetValue;

/**
 * @author andi
 * 
 */
public class FacetValue implements IFacetValue {

	private String m_domain;
	private double m_nodeId;

	private String m_value;
	private String m_ext;

	public String getDomain() {
		return m_domain;
	}

	public String getExt() {
		return m_ext;
	}

	public double getNodeId() {
		return m_nodeId;
	}

	public String getValue() {
		return m_value;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setExt(String ext) {
		m_ext = ext;
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
