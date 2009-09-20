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
package edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance;

import java.math.BigDecimal;
import java.util.ArrayList;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.IFacetClusterDistance;

/**
 * @author andi
 * 
 */
public class ClusterDistance implements IFacetClusterDistance {

	private BigDecimal m_value;

	private String m_leftBorder;
	private String m_rightBorder;

	private int m_leftCountS;
	private int m_leftCountFV;

	private ArrayList<ClusterDistance> m_leftDistances;

	public ClusterDistance(String leftBorder, String rightBorder) {

		m_leftBorder = leftBorder;
		m_rightBorder = rightBorder;

		// init stuff
		m_leftDistances = new ArrayList<ClusterDistance>();		
		
	}

	/**
	 * @return the leftBorder
	 */
	public String getLeftBorder() {
		return m_leftBorder;
	}

	/**
	 * @return the leftCountFV
	 */
	public int getLeftCountFV() {
		return m_leftCountFV;
	}

	/**
	 * @return the leftCountS
	 */
	public int getLeftCountS() {
		return m_leftCountS;
	}

	/**
	 * @return the leftDistances
	 */
	public ArrayList<ClusterDistance> getLeftDistances() {
		return m_leftDistances;
	}

	/**
	 * @return the rightBorder
	 */
	public String getRightBorder() {
		return m_rightBorder;
	}

	/**
	 * @return the value
	 */
	public BigDecimal getValue() {
		return m_value;
	}

	/**
	 * @param leftBorder
	 *            the leftBorder to set
	 */
	public void setLeftBorder(String leftBorder) {
		m_leftBorder = leftBorder;
	}

	/**
	 * @param leftCountFV
	 *            the leftCountFV to set
	 */
	public void setLeftCountFV(int leftCountFV) {
		m_leftCountFV = leftCountFV;
	}

	/**
	 * @param leftCountS
	 *            the leftCountS to set
	 */
	public void setLeftCountS(int leftCountS) {
		m_leftCountS = leftCountS;
	}

	/**
	 * @param leftDistances
	 *            the leftDistances to set
	 */
	public void setLeftDistances(ArrayList<ClusterDistance> leftDistances) {
		m_leftDistances = leftDistances;
	}

	/**
	 * @param rightBorder
	 *            the rightBorder to set
	 */
	public void setRightBorder(String rightBorder) {
		m_rightBorder = rightBorder;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(BigDecimal value) {
		m_value = value;
	}

}
