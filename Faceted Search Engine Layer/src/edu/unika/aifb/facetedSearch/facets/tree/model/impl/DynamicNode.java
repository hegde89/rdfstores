package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.util.List;

import edu.unika.aifb.facetedSearch.facets.tree.model.IDynamicNode;

public class DynamicNode extends StaticNode implements IDynamicNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3453365940192769709L;

	private String m_leftBorder;
	private String m_rightBorder;
	private List<String> m_lits;

	public DynamicNode() {
		super();
	}

	public DynamicNode(String value, NodeContent content) {
		super(value, content);
	}

	public DynamicNode(String value, NodeType type) {
		super(value, type);
	}

	public DynamicNode(String value, NodeType type, NodeContent content) {
		super(value, type, content);
	}

	public String getLeftBorder() {
		return m_leftBorder;
	}

	public List<String> getLits() {
		return m_lits;
	}

	public String getRightBorder() {
		return m_rightBorder;
	}

	public void setLeftBorder(String leftValue) {
		m_leftBorder = leftValue;
	}

	public void setLits(List<String> lits) {
		m_lits = lits;
	}

	public void setRightBorder(String rightValue) {
		m_rightBorder = rightValue;
	}
}
