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

import edu.unika.aifb.facetedSearch.api.model.IAbstractObject;
import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class Individual extends AbstractObject implements IIndividual {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4857788292598284020L;

	public Individual(String value, String extension) {
		super(value, extension);
	}

	public Individual(SearchSession session, String value, String extension) {
		super(value, extension);
		super.setSession(session);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.api.objects.IIndividual#getObjects()
	 */
	public Map<String, IAbstractObject> getObjects() {
		return getSession() == null ? null : getSession().getStore()
				.getObjects(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.api.objects.IIndividual#getURI()
	 */
	public String getURI() {
		return getValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.api.objects.IIndividual#setURI(java.lang
	 * .String)
	 */
	public void setURI(String uri) {
		setValue(uri);
	}
}
