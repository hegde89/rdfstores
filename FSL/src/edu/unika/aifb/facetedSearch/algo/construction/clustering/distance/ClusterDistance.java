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
package edu.unika.aifb.facetedSearch.algo.construction.clustering.distance;

import java.io.Serializable;
import java.math.BigDecimal;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.IFacetClusterDistance;

/**
 * @author andi
 * 
 */
public class ClusterDistance implements IFacetClusterDistance, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1051678213418421275L;

	/*
	 * 
	 */
	private BigDecimal m_value;

	/*
	 * 
	 */
	private String m_leftBorder;
	private String m_rightBorder;

	/*
	 * 
	 */
	private int leftIdx;
	private int rightIdx;

	/*
	 * 
	 */
	private int m_leftCountS;
	private int m_leftCountFV;

	public ClusterDistance(String leftBorder, String rightBorder) {
		m_leftBorder = leftBorder;
		m_rightBorder = rightBorder;
	}

	public String getLeftBorder() {
		return m_leftBorder;
	}

	public int getLeftCountFV() {
		return m_leftCountFV;
	}

	public int getLeftCountS() {
		return m_leftCountS;
	}

	public int getLeftIdx() {
		return leftIdx;
	}

	public String getRightBorder() {
		return m_rightBorder;
	}

	public int getRightIdx() {
		return rightIdx;
	}

	public BigDecimal getValue() {
		return m_value;
	}

	public void setLeftBorder(String leftBorder) {
		m_leftBorder = leftBorder;
	}

	public void setLeftCountFV(int leftCountFV) {
		m_leftCountFV = leftCountFV;
	}

	public void setLeftCountS(int leftCountS) {
		m_leftCountS = leftCountS;
	}

	public void setLeftIdx(int leftIdx) {
		this.leftIdx = leftIdx;
	}

	public void setRightBorder(String rightBorder) {
		m_rightBorder = rightBorder;
	}

	public void setRightIdx(int rightIdx) {
		this.rightIdx = rightIdx;
	}

	public void setValue(BigDecimal value) {
		m_value = value;
	}
}
