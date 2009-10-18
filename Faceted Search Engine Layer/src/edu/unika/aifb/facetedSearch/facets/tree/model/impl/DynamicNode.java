package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.IDynamicNode;

public class DynamicNode extends StaticNode implements IDynamicNode {

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode#getCountFV
	 * ()
	 */
	@Override
	public int getCountFV() {

		if (m_countFV == -1) {
			m_countFV = super.getCache().getCountFV4DynNode(this);
		}

		return m_countFV;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode#getCountS
	 * ()
	 */
	@Override
	public int getCountS() {

		if (m_countS == -1) {
			m_countS = super.getCache().getCountS4DynNode(this);
		}

		return m_countS;
	}

	public String getLeftBorder() {
		return m_leftBorder;
	}

	public List<AbstractSingleFacetValue> getLiterals() {
		return super.getCache().getLiterals4DynNode(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode#getObjects
	 * ()
	 */
	@Override
	@Deprecated
	public Set<AbstractSingleFacetValue> getObjects() {
		return new HashSet<AbstractSingleFacetValue>(super.getCache()
				.getLiterals4DynNode(this));
	}

	public String getRightBorder() {
		return m_rightBorder;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode#getSources
	 * ()
	 */
	@Override
	public Set<String> getSources() throws DatabaseException, IOException {
		return super.getCache().getSources4DynNode(this);
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

	public void setLiterals(List<AbstractSingleFacetValue> lits) {
		super.getCache().storeLiterals(this, lits);
	}

	public void setRightBorder(String rightValue) {
		m_rightBorder = rightValue;
	}
}
