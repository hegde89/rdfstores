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

import edu.unika.aifb.facetedSearch.facets.model.IAbstractBrowsingObject;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public abstract class AbstractBrowsingObject
		implements
			IAbstractBrowsingObject,
			Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1362655078324640467L;

	/*
	 * 
	 */
	private int m_countS;
	private String m_domain;
	private double m_nodeId;
	private String m_value;

	public AbstractBrowsingObject() {
		m_countS = 0;
	}

	public AbstractBrowsingObject(String value) {
		m_value = value;
		m_countS = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof AbstractBrowsingObject) {

			return ((AbstractBrowsingObject) obj).getNodeId() == getNodeId();

		} else {
			return false;
		}
	}

	public int getCountS() {
		return m_countS;
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

	public void setCountS(int countS) {
		m_countS = countS;
	}

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

		if (m_value != null) {
			return Util.truncateUri(m_value.toLowerCase()) + " (" + m_countS
					+ ")";
		} else {
			return "N/A";
		}
	}
}
