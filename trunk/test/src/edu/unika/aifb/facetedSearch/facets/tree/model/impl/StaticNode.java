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

/**
 * @author andi
 * 
 */
public class StaticNode extends Node {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5743733958338988213L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StaticNode.class);

	/*
	 * 
	 */
	protected int m_countFV;

	protected int m_countS;
	private int m_height;
	private int m_depth;
	private int m_size;

	private boolean m_isTypeLeave;

	public StaticNode() {
		super();
		init();
	}

	public StaticNode(String value) {
		super(value);
		init();
	}

	public StaticNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public int getDepth() {
		return m_depth;
	}

	public int getHeight() {
		return m_height;
	}

	public int getSize() {
		return m_size;
	}

	private void init() {

		m_isTypeLeave = false;
		m_countFV = -1;
		m_countS = -1;
		m_height = -1;
		m_size = -1;
	}

	public boolean isTypeLeave() {
		return m_isTypeLeave;
	}

	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	public void setCountS(int countS) {
		m_countS = countS;
	}

	public void setDepth(int depth) {
		m_depth = depth;
	}

	public void setHeight(int height) {
		m_height = height;
	}

	public void setSize(int size) {
		m_size = size;
	}

	public void setTypeLeave(boolean isTypeLeave) {
		m_isTypeLeave = isTypeLeave;
	}
}
