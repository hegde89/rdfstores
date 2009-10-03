/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;

/**
 * @author andi
 * 
 */
public class Node implements INode {

	private static final long serialVersionUID = -1484769190480836362L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(Node.class);

	/*
	 * 
	 */
	private Facet m_facet;

	/*
	 * 
	 */
	private int m_type;
	private int m_content;

	/*
	 * 
	 */
	private HashSet<String> m_RangeExtensions;
	private HashSet<String> m_SourceExtensions;

	/*
	 * 
	 */
	private double m_weight;
	private String m_value;
	private String m_domain;

	/*
	 * 
	 */
	private int m_pathHashValue;
	private String m_path;

	/*
	 * 
	 */
	private double m_id;
	private List<Double> m_leaves;

	public Node() {
		init();
	}

	public Node(String value) {

		m_value = value;
		init();
	}

	public Node(String value, int type, int content) {

		m_type = type;
		m_value = value;
		m_content = content;

		init();
	}

	public boolean addLeave(double leave) {

		if (!m_leaves.contains(leave)) {
			return m_leaves.add(leave);
		} else {
			return false;
		}
	}

	@Override
	public boolean equals(Object object) {

		return object instanceof INode ? ((INode) object).getID() == this
				.getID() : false;
	}

	public int getContent() {
		return m_content;
	}

	public String getDomain() {
		return m_domain;
	}

	public Facet getFacet() {
		return m_facet;
	}

	public double getID() {
		return this.m_id;
	}

	public List<Double> getLeaves() {
		return m_leaves;
	}

	public String getPath() {
		return m_path;
	}

	public int getPathHashValue() {
		return m_pathHashValue;
	}

	public HashSet<String> getRangeExtensions() {
		return m_RangeExtensions;
	}

	public HashSet<String> getSourceExtensions() {
		return m_SourceExtensions;
	}

	public int getType() {
		return m_type;
	}

	public String getValue() {
		return m_value;
	}

	public double getWeight() {
		return m_weight;
	}

	public boolean hasPath() {
		return m_path != null;
	}

	public boolean hasPathHashValue() {
		return m_pathHashValue != Integer.MIN_VALUE;
	}

	public boolean hasSameValueAs(Object object) {

		return object instanceof INode ? ((INode) object).getValue().equals(
				this.getValue()) : false;
	}

	private void init() {

		m_id = (new Random()).nextGaussian();

		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();

		m_leaves = new ArrayList<Double>();

		m_pathHashValue = Integer.MIN_VALUE;
		m_path = null;
	}

	public boolean isInnerNode() {
		return m_type == NodeType.INNER_NODE;
	}

	public boolean isLeave() {
		return m_type == NodeType.LEAVE;
	}

	public boolean isRangeRoot() {
		return m_type == NodeType.RANGE_ROOT;
	}

	public boolean isRoot() {
		return m_type == NodeType.ROOT;
	}

	public Facet makeFacet(String uri, int ftype, int dtype) {
		return new Facet(uri, ftype, dtype);
	}

	public void setContent(int content) {
		m_content = content;
	}

	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setFacet(Facet facet) {
		m_facet = facet;
	}

	public void setID(double id) {
		this.m_id = id;
	}

	public void setLeaves(List<Double> leaves) {
		m_leaves = leaves;
	}

	public void setPath(String path) {
		m_path = path;
	}

	public void setPathHashValue(int pathHashValue) {
		m_pathHashValue = pathHashValue;
	}

	public void setType(int type) {
		m_type = type;
	}

	public void setValue(String value) {
		this.m_value = value;
	}

	public void setWeight(double weight) {
		this.m_weight = weight;
	}

	@Override
	public String toString() {
		return "Node" + m_id + " :[Label:" + m_value + ", Type:" + m_type
				+ ", Content:" + m_content + ", Extensions: "
				+ m_RangeExtensions + "]";
	}
}
