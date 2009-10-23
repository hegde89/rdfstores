package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import edu.unika.aifb.facetedSearch.FacetEnvironment;

public class DynamicNode extends StaticNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3453365940192769709L;

	/*
	 * 
	 */
	private String m_leftBorder;
	private String m_rightBorder;

	/*
	 * 
	 */
	private int m_calClusterDepth;

	/*
	 * 
	 */
	private boolean m_hasCalChildren;

	public DynamicNode() {
		super();
		init();
	}

	public DynamicNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public int getCalClusterDepth() {
		return m_calClusterDepth;
	}

	public String getLeftBorder() {
		return m_leftBorder;
	}

	public String getRightBorder() {
		return m_rightBorder;
	}

	public boolean hasCalChildren() {
		return m_hasCalChildren;
	}

	private void init() {
		m_calClusterDepth = FacetEnvironment.CalClusterDepth.NOT_SET;
		m_hasCalChildren = true;
	}

	public void setCalClusterDepth(int calClusterDepth) {
		m_calClusterDepth = calClusterDepth;
	}

	public void setHasCalChildren(boolean hasCalChildren) {
		m_hasCalChildren = hasCalChildren;
	}

	public void setLeftBorder(String leftValue) {
		m_leftBorder = leftValue;
	}

	public void setRightBorder(String rightValue) {
		m_rightBorder = rightValue;
	}
}