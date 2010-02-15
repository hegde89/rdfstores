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
package edu.unika.aifb.facetedSearch.facets.model.impl;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.facets.model.IFacet;

/**
 * @author andi
 * 
 */
public class Facet extends AbstractBrowsingObject implements IFacet {

	public static final Facet NULL = new Facet("null", -1, -1);

	/**
	 * 
	 */
	private static final long serialVersionUID = -7228202054063357449L;

	/*
	 * 
	 */
	private int m_facetType;
	private int m_dataType;

	public Facet() {

		super();
		init();
	}

	public Facet(String uri) {

		super(uri);
		init();
	}

	public Facet(String uri, int ftype, int dtype) {

		super(uri);
		m_facetType = ftype;
		m_dataType = dtype;
	}

	public int getDataType() {
		return m_dataType;
	}

	@Override
	public int getType() {
		return m_facetType;
	}

	public String getUri() {
		return super.getValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject
	 * #getValue()
	 */
	@Override
	@Deprecated
	public String getValue() {
		return super.getValue();
	}

	private void init() {
		m_facetType = FacetType.NOT_SET;
		m_dataType = DataType.NOT_SET;
	}

	public boolean isDataPropertyBased() {
		return m_facetType == FacetType.DATAPROPERTY_BASED;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject
	 * #isLeave()
	 */
	@Override
	public boolean isLeave() {
		return false;
	}

	public boolean isObjectPropertyBased() {
		return m_facetType == FacetType.OBJECT_PROPERTY_BASED;
	}

	public void setDataType(int dataType) {
		m_dataType = dataType;
	}

	@Override
	public void setType(int type) {
		m_facetType = type;
	}

	public void setUri(String uri) {
		super.setValue(uri);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject
	 * #setValue(java.lang.String)
	 */
	@Override
	@Deprecated
	public void setValue(String value) {
		super.setValue(value);
	}
}
