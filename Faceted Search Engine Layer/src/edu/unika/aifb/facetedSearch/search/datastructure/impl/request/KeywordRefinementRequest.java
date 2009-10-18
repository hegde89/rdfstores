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
package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

/**
 * @author andi
 * 
 */
public class KeywordRefinementRequest extends AbstractRefinementRequest {

	private String m_domain;
	private double m_facetID;
	private String m_keywords;

	public KeywordRefinementRequest() {
		super("keywordRefinementRequest");
	}

	public KeywordRefinementRequest(String name) {
		super(name);
	}

	public String getDomain() {
		return m_domain;
	}

	public double getFacetID() {
		return m_facetID;
	}

	public String getKeywords() {
		return m_keywords;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setFacetID(double facetID) {
		m_facetID = facetID;
	}

	public void setKeywords(String keywords) {
		m_keywords = keywords;
	}

}
