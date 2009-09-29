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

import java.util.HashSet;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;

/**
 * @author andi
 * 
 */
public class Node implements INode {

	public enum NodeContent {
		TYPE_PROPERTY, DATA_PROPERTY, OBJECT_PROPERTY, CLASS
	}

	public enum NodeType {
		ROOT, RANGE_ROOT, INNER_NODE, LEAVE
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1484769190480836362L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(Node.class);

	private Facet m_facet;
	private double m_weight;
	private String m_value;
	private NodeType m_type;
	private NodeContent m_content;
	private HashSet<String> m_RangeExtensions;
	private HashSet<String> m_SourceExtensions;
	private String m_domain;
	private int m_pathHashValue = Integer.MIN_VALUE;
	private String m_path = null;

	private double m_id;

	public Node() {

		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();
	}

	public Node(String value, NodeContent content) {

		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();

	}

	public Node(String value, NodeType type) {

		m_type = type;
		m_value = value;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();

	}

	public Node(String value, NodeType type, NodeContent content) {

		m_type = type;
		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();
		
	}

	// public void addRangeExtension(String extension) {
	// m_RangeExtensions.add(extension);
	// }
	//
	// public void addRangeExtensions(Collection<String> extensions) {
	// m_RangeExtensions.addAll(extensions);
	// }
	//
	// public void addRangeExtensions(String extensions) {
	// m_RangeExtensions.addAll(FacetUtils.string2List(extensions));
	// }
	//
	// public void addSourceExtension(String extension) {
	// m_SourceExtensions.add(extension);
	// }
	//
	// public void addSourceExtensions(Collection<String> extensions) {
	// m_SourceExtensions.addAll(extensions);
	// }
	//
	// public void addSourceExtensions(String extensions) {
	// m_SourceExtensions.addAll(FacetUtils.string2List(extensions));
	// }

	// @Override
	@Override
	public boolean equals(Object object) {

		return object instanceof INode ? ((INode) object).getID() == this
				.getID() : false;
	}

	/**
	 * @return the m_content
	 */
	public NodeContent getContent() {
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

	public NodeType getType() {
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

	public Facet makeFacet(String uri, FacetType ftype, DataType dtype) {
		return new Facet(uri, ftype, dtype);
	}

	public void setContent(NodeContent content) {
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

	public void setPath(String path) {
		m_path = path;
	}

	public void setPathHashValue(int pathHashValue) {
		m_pathHashValue = pathHashValue;
	}

	public void setType(NodeType type) {
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
		return "Node" + m_id + " :[Label:" + m_value + ", Type:"
				+ m_type + ", Content:" + m_content
				+ ", Extensions: " + m_RangeExtensions + "]";
	}
}
