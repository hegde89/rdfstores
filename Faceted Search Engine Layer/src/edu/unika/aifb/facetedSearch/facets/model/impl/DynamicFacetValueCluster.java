package edu.unika.aifb.facetedSearch.facets.model.impl;

import java.util.Map;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.facets.model.IDynamicFacetValueCluster;
import edu.unika.aifb.facetedSearch.facets.model.INode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

public class DynamicFacetValueCluster extends FacetValueCluster implements
		IDynamicFacetValueCluster {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3453365940192769709L;


	protected DynamicFacetValueCluster(SearchSession session) {
		super(session);
	}

	private INode m_leftBorder;
	private INode m_rightBorder;

	
	public INode getLeftBorder() {
		return m_leftBorder;
	}

	public INode getRightBorder() {
		return m_rightBorder;
	}

	public void setLeftBorder(INode leftValue) {
		m_leftBorder = leftValue;
	}

	public void setRightBorder(INode rightValue) {
		m_rightBorder = rightValue;
	}

	@Override
	public Map<IIndividual, Integer> getSources() {
		// TODO GET SOURCES IMPORTANT
		return null;
	}
}
