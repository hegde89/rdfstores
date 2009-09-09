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
package edu.unika.aifb.facetedSearch.index.distance.model.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;

/**
 * @author andi
 * 
 */
public class LiteralList implements Iterable<ILiteral>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2846409399565269239L;

	private DataType m_dataType;
	private List<ILiteral> m_literals;

	public LiteralList() {
		m_literals = new ArrayList<ILiteral>();
	}

	public LiteralList(DataType type) {
		m_literals = new ArrayList<ILiteral>();
		m_dataType = type;
	}

	public LiteralList(DataType type, List<ILiteral> literals) {
		m_literals = literals;
		m_dataType = type;
	}

	public LiteralList(List<ILiteral> literals) {
		m_literals = literals;
	}

	public boolean add(ILiteral lit) {
		return m_literals.add(lit);
	}

	public boolean addAll(Collection<? extends ILiteral> lits) {
		return m_literals.addAll(lits);
	}

	public boolean contains(ILiteral lit) {
		return m_literals.contains(lit);
	}

	/**
	 * @return the dataType
	 */
	public DataType getDataType() {
		return m_dataType;
	}

	/**
	 * @return the literals
	 */
	public List<ILiteral> getLiterals() {
		return m_literals;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<ILiteral> iterator() {
		return m_literals.iterator();
	}

	/**
	 * @param dataType
	 *            the dataType to set
	 */
	public void setDataType(DataType dataType) {
		m_dataType = dataType;
	}

	/**
	 * @param literals
	 *            the literals to set
	 */
	public void setLiterals(List<ILiteral> literals) {
		m_literals = literals;
	}

	public int size() {
		return m_literals.size();
	}

	public String toString() {
		return "[Datatype: '" + m_dataType + "', Literals:" + m_literals + "]";
	}

}
