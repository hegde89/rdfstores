package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;

public class DynamicClusterNode extends StaticClusterNode {

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

	/*
	 * 
	 */
	private String m_calPrefix;

	/*
	 * 
	 */
	private double m_heightIndicator;

	public DynamicClusterNode() {
		super();
		init();
	}

	public DynamicClusterNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public int getCalClusterDepth() {
		return m_calClusterDepth;
	}

	public String getCalPrefix() {
		return m_calPrefix;
	}

	public double getHeightIndicator() {
		return m_heightIndicator;
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
		
		m_heightIndicator = FacetedSearchLayerConfig.DefaultValue.HEIGHT_INDICATOR;
		m_calClusterDepth = FacetEnvironment.CalClusterDepth.NOT_SET;
		m_hasCalChildren = true;
	}

	public void setCalClusterDepth(int calClusterDepth) {
		m_calClusterDepth = calClusterDepth;
	}

	public void setCalPrefix(String calPrefix) {
		m_calPrefix = calPrefix;
	}

	public void setHasCalChildren(boolean hasCalChildren) {
		m_hasCalChildren = hasCalChildren;
	}

	public void setHeightIndicator(double heightIndicator) {
		m_heightIndicator = heightIndicator;
	}

	public void setLeftBorder(String leftValue) {
		m_leftBorder = leftValue;
	}

	public void setRightBorder(String rightValue) {
		m_rightBorder = rightValue;
	}
}