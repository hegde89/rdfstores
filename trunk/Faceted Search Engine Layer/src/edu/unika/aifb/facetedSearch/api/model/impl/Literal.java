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

import edu.unika.aifb.facetedSearch.Environment.DataType;
import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.util.Util;

/**
 * @author andi
 * 
 */
public class Literal extends AbstractObject implements ILiteral {

	private DataType m_dataType;
	// only value, no data-type
	private String m_value;

	protected Literal(SearchSession session, String value, String extension) {
		super(session, value, extension);
		this.m_value = Util.getValueOfLiteral(this);
	}

	public DataType getDataType() {
		return this.m_dataType == null ? this.m_dataType = Util
				.getDataType(this) : this.m_dataType;
	}

	public Map<String, IIndividual> getSubjects() {
		return this.getSession().getStore().getSubjects(this);
	}

	@Override
	public String getValue() {
		return this.m_value;
	}

	public void setDataType(DataType type) {
		this.m_dataType = type;
	}

	@Override
	public String toString() {
		return this.m_value;
	}
}
