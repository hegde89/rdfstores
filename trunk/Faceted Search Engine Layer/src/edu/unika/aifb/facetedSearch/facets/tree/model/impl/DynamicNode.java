package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import edu.unika.aifb.facetedSearch.facets.tree.model.IDynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;

public class DynamicNode extends StaticNode implements
		IDynamicNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3453365940192769709L;

	private INode m_leftBorder;

	private INode m_rightBorder;

	public DynamicNode(String value, NodeContent content) {
		super(value, content);
	}

	public DynamicNode(String value, NodeType type) {
		super(value, type);
	}

	public DynamicNode(String value, NodeType type, NodeContent content) {
		super(value, type, content);
	}

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
}
