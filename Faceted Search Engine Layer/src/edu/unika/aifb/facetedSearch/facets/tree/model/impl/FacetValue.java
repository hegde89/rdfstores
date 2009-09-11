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
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import edu.unika.aifb.facetedSearch.facets.tree.model.IFacetValue;

/**
 * @author andi
 * 
 */
public class FacetValue extends Node implements IFacetValue {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4611143426289856264L;

	private int m_countFV;
	private int m_countS;
	private String m_name;

	public FacetValue(String value) {
		super(value, NodeType.LEAVE);
	}

	public FacetValue(String value, NodeContent content) {
		super(value, NodeType.LEAVE, content);
	}

	public int getCountFV() {
		return m_countFV;
	}

	public int getCountS() {
		return m_countS;
	}

	public String getName() {
		return m_name;
	}

	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	public void setCountS(int countS) {
		m_countS = countS;
	}

	public void setName(String name) {
		m_name = name;
	}

	// @Override
	// public List<IFacetValueTuple> getChildren() {
	// return null;
	// }
	//	
	// @Override
	// public List<IFacetValueTuple> getChildren(boolean rankingEnabled) {
	// return null;
	// }
}
