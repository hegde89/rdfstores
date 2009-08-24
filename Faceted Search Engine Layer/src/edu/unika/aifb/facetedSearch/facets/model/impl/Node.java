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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.model.IFacetValueTuple;
import edu.unika.aifb.facetedSearch.facets.model.INode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public abstract class Node implements INode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2768224446389016484L;
	private String m_SourceExtension;
	private List<String> m_FVExtensions;
	private FacetTreeDelegator m_facetTreeDelegator;
	private double m_id;

	protected Node(SearchSession session) {
		this.m_FVExtensions = new ArrayList<String>();
		this.m_facetTreeDelegator = session.getFacetTreeDelegator();
		this.m_id = (new Random()).nextGaussian();
	}

	public void addFVExtension(String extension) {
		this.m_FVExtensions.add(extension);
	}

	public List<IFacetValueTuple> getChildren() {
		return this.m_facetTreeDelegator.getFacetValueTuples(this);
	}

	public List<IFacetValueTuple> getChildren(boolean rankingEnabled) {
		return this.m_facetTreeDelegator.getFacetValueTuples(this);
	}

	public Node getFather() {

		Edge edge2father = this.getTree().incomingEdgesOf(this).iterator()
				.next();

		return this.getTree().getEdgeSource(edge2father);
	}

	public List<String> getFVExtensions() {
		return this.m_FVExtensions;
	}

	/**
	 * @return the id
	 */
	public double getID() {
		return this.m_id;
	}

	public String getSourceExtension() {
		return this.m_SourceExtension;
	}

	public FacetTree<Node, Edge> getTree() {
		return this.m_facetTreeDelegator.getFacetTree(this.m_SourceExtension);
	}

	public void setFVExtensions(List<String> extensions) {
		this.m_FVExtensions = extensions;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setID(double id) {
		this.m_id = id;
	}

	public void setSourceExtension(String extension) {
		this.m_SourceExtension = extension;
	}
}
