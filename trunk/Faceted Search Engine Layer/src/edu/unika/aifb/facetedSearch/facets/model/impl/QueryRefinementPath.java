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

import java.util.Random;

import edu.unika.aifb.facetedSearch.facets.model.IRefinementPath;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class QueryRefinementPath implements IRefinementPath {

	private StructuredQuery m_structuredQuery;
	private double m_id;
	private String m_domain;

	public QueryRefinementPath(String domain, StructuredQuery structuredQuery) {

		m_domain = domain;
		setStructuredQuery(structuredQuery);
		m_id = (new Random()).nextGaussian();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.IRefinementPath#getDomain()
	 */
	@Override
	public String getDomain() {
		return m_domain;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.facets.model.IRefinementPath#getId()
	 */
	@Override
	public double getId() {
		return m_id;
	}

	public StructuredQuery getStructuredQuery() {
		return m_structuredQuery;
	}

	public void setStructuredQuery(StructuredQuery structuredQuery) {
		m_structuredQuery = structuredQuery;
	}

}
