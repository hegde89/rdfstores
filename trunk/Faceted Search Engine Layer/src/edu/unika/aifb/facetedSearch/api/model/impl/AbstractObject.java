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

import edu.unika.aifb.facetedSearch.Environment.ObjectType;
import edu.unika.aifb.facetedSearch.api.model.IAbstractObject;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.util.Util;

/**
 * @author andi
 * 
 */
public abstract class AbstractObject implements IAbstractObject {

	private String m_extension;
	// only value, incl. data-type (if Literal)
	private String m_value;
	private SearchSession m_session;

	protected AbstractObject(SearchSession session, String value,
			String extension) {
		this.m_value = value;
		this.setSession(session);
		this.setExtension(extension);
	}

	@Override
	public boolean equals(Object object) {

		if (object instanceof IAbstractObject) {
			return ((IAbstractObject) object).getValue()
					.equals(this.getValue());
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.api.objects.IEntity#getExtension()
	 */
	public String getExtension() {
		return this.m_extension;
	}

	/**
	 * @return the session
	 */
	public SearchSession getSession() {
		return this.m_session;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.api.objects.IEntity#getType()
	 */
	public ObjectType getType() {

		return Util.isDataValue(this.m_value) ? ObjectType.LITERAL
				: ObjectType.INDIVIDUAL;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.api.objects.IEntity#getValue()
	 */
	public String getValue() {
		return this.m_value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.api.objects.IEntity#setExtension(java.lang
	 * .String)
	 */
	public void setExtension(String extension) {
		this.m_extension = extension;

	}

	/**
	 * @param session
	 *            the session to set
	 */
	public void setSession(SearchSession session) {
		this.m_session = session;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.api.objects.IEntity#setValue(java.lang.String
	 * )
	 */
	public void setValue(String value) {
		this.m_value = value;
	}

	@Override
	public String toString() {
		return this.m_value;
	}
}
