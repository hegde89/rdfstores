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

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.IAbstractBrowsingObject;

/**
 * @author andi
 * 
 */
public abstract class AbstractBrowsingObject
		implements
			IAbstractBrowsingObject,
			Serializable,
			Comparable<AbstractBrowsingObject> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1362655078324640467L;

	/*
	 * 
	 */
	private int m_countS;
	private int m_countFV;

	private double m_weight;

	/*
	 * 
	 */
	private String m_domain;
	private String m_value;
	private String m_label;

	/*
	 * 
	 */
	private int m_content;
	private int m_type;

	/*
	 * 
	 */
	private double m_nodeId;

	public AbstractBrowsingObject() {
		init();
	}

	public AbstractBrowsingObject(String value) {
		m_value = value;
		init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(AbstractBrowsingObject obj) {

		if (obj.getWeight() < getWeight()) {
			return -1;
		} else if (obj.getWeight() > getWeight()) {
			return 1;
		} else {
			return 0;
		}
	}

	// /*
	// * (non-Javadoc)
	// *
	// * @see java.lang.Object#equals(java.lang.Object)
	// */
	// @Override
	// public boolean equals(Object obj) {
	//
	// if (obj instanceof AbstractBrowsingObject) {
	//
	// return ((AbstractBrowsingObject) obj).getValue().equals(getValue());
	//
	// } else {
	// return false;
	// }
	// }

	public int getContent() {
		return m_content;
	}

	public int getCountFV() {
		return m_countFV;
	}

	public int getCountS() {
		return m_countS;
	}

	public String getDomain() {
		return m_domain;
	}

	public String getLabel() {
		return m_label;
	}

	public double getNodeId() {
		return m_nodeId;
	}

	public int getType() {
		return m_type;
	}

	public String getValue() {
		return m_value;
	}

	public double getWeight() {
		return m_weight;
	}

	// /*
	// * (non-Javadoc)
	// *
	// * @see java.lang.Object#hashCode()
	// */
	// @Override
	// public int hashCode() {
	// return m_value.hashCode();
	// }

	public boolean hasLabel() {
		return !m_label.equals(FacetEnvironment.DefaultValue.NO_LABEL);
	}

	private void init() {

		m_countS = 0;
		m_countFV = 0;
		m_weight = 0;
		m_nodeId = 0;
		m_label = FacetEnvironment.DefaultValue.NO_LABEL;
	}

	public abstract boolean isLeave();

	public void setContent(int content) {
		m_content = content;
	}

	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	public void setCountS(int countS) {
		m_countS = countS;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setLabel(String label) {
		m_label = label;
	}

	public void setNodeId(double nodeId) {
		m_nodeId = nodeId;
	}

	public void setType(int type) {
		m_type = type;
	}

	public void setValue(String value) {
		m_value = value;
	}

	public void setWeight(double weight) {
		m_weight = weight;
	}

	@Override
	public String toString() {
		return m_value;
	}
}