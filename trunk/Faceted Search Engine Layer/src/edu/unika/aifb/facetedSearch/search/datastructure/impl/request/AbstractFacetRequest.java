package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

import edu.unika.aifb.graphindex.query.Query;

public abstract class AbstractFacetRequest extends Query {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9104287126010463316L;
	
	private String m_userID;

	protected AbstractFacetRequest(String name) {
		super(name);
	}

	public String getUserID() {
		return m_userID;
	}

	public void setUserID(String userID) {
		m_userID = userID;
	}
}
