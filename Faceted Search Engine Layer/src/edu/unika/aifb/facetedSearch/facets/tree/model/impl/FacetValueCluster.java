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

import java.util.Map;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.facets.tree.model.IFacetValueCluster;

/**
 * @author andi
 * 
 */
public abstract class FacetValueCluster extends Node implements
		IFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5743733958338988213L;

	private int m_countFV;

	private int m_countS;
	private String m_name;
	private int m_height;
	private int m_size;

	public FacetValueCluster(String value, NodeContent content) {
		super(value, content);
	}

	public FacetValueCluster(String value, NodeType type) {
		super(value, type);
	}

	public FacetValueCluster(String value, NodeType type, NodeContent content) {
		super(value, type, content);
	}

	public int getCountFV() {
		return this.m_countFV;
	}

	public int getCountS() {
		return this.m_countS;
	}

	public int getHeight() {
		return this.m_height;
	}

	public String getName() {
		return this.m_name;
	}

	public int getSize() {
		return this.m_size;
	}

	public abstract Map<IIndividual, Integer> getSources();

	public void setCountFV(int countFV) {
		this.m_countFV = countFV;
	}

	public void setCountS(int countS) {
		this.m_countS = countS;
	}

	public void setHeight(int height) {
		this.m_height = height;
	}

	public void setName(String name) {
		this.m_name = name;
	}

	public void setSize(int size) {
		this.m_size = size;
	}
}
