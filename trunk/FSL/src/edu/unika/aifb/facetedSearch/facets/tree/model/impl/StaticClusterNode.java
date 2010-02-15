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

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.facets.tree.model.IStaticNode;

/**
 * @author andi
 * 
 */
public class StaticClusterNode extends Node implements IStaticNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5743733958338988213L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StaticClusterNode.class);

	/*
	 * 
	 */
	private double m_height;

	public StaticClusterNode() {
		super();
		init();
	}

	public StaticClusterNode(String value) {
		super(value);
		init();
	}

	public StaticClusterNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public double getHeight() {
		return m_height;
	}

	public boolean hasHeight() {
		return m_height != Double.MIN_VALUE;
	}

	private void init() {
		m_height = Double.MIN_VALUE;
	}

	public void setHeight(double height) {
		m_height = height;
	}
}