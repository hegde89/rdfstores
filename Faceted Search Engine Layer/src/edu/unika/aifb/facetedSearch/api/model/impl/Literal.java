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
package edu.unika.aifb.facetedSearch.api.model.impl;

import java.util.Map;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class Literal extends AbstractObject implements ILiteral {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9061190356771959180L;
	private DataType m_dataType;

	public Literal(SearchSession session, String value, String extension) {
		super(value, extension);
		super.setSession(session);
	}

	public Literal(String value) {
		super(value);
	}

	public Literal(String value, DataType type) {
		super(value);
		m_dataType = type;
	}

	public Literal(String value, String extension) {
		super(value, extension);
	}

	public DataType getDataType() {
		return m_dataType;
	}

	public Map<String, IIndividual> getSubjects() {
		return super.getSession() == null ? null : super.getSession()
				.getStore().getSubjects(this);
	}

	public void setDataType(DataType type) {
		m_dataType = type;
	}

	@Override
	public String toString() {
		return super.getValue() + FacetEnvironment.DefaultValue.LITERAL_DELIM
				+ m_dataType;
	}
}
