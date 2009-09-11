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
		ROOT, RANGE_ROOT, INNER_NODE, LEAVE, ENDPOINT
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1484769190480836362L;

	private double weight;
	private String m_value;
	private HashSet<NodeType> m_types;
	private NodeContent m_content;
	private List<String> m_RangeExtensions;
	private List<String> m_SourceExtensions;
	private String m_domain;
	private int m_pathHashValue = Integer.MIN_VALUE;

	private double m_id;

	public Node(String value, HashSet<NodeType> types) {

		m_value = value;
		m_types = new HashSet<NodeType>();
		m_types.addAll(types);
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new ArrayList<String>();
		m_SourceExtensions = new ArrayList<String>();
		// setPathHashValue(pathHashValue);
	}

	public Node(String value, NodeContent content) {

		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new ArrayList<String>();
		m_SourceExtensions = new ArrayList<String>();
		// setPathHashValue(pathHashValue);
	}

	public Node(String value, NodeType type) {

		m_types = new HashSet<NodeType>();
		m_types.add(type);
		m_value = value;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new ArrayList<String>();
		m_SourceExtensions = new ArrayList<String>();
		// setPathHashValue(pathHashValue);
	}

	public Node(String value, NodeType type, NodeContent content) {

		m_types = new HashSet<NodeType>();
		m_types.add(type);
		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new ArrayList<String>();
		m_SourceExtensions = new ArrayList<String>();
		// setPathHashValue(pathHashValue);
	}

	public void addRangeExtension(String extension) {
		this.m_RangeExtensions.add(extension);
	}

	public void addRangeExtensions(List<String> extensions) {
		this.m_RangeExtensions.addAll(extensions);
	}

	// public Node() {
	//
	// this.m_id = (new Random()).nextGaussian();
	// this.m_extensions = new ArrayList<String>();
	// }
	//
	// public Node(NodeType type) {
	//
	// this.m_type = type;
	// this.m_id = (new Random()).nextGaussian();
	// this.m_extensions = new ArrayList<String>();
	// }

	public void addType(NodeType type) {
		m_types.add(type);
	}

	@Override
	public boolean equals(Object object) {

		return object instanceof INode ? ((INode) object).getValue().equals(
				this.getValue()) : false;
	}

	/**
	 * @return the m_content
	 */
	public NodeContent getContent() {
		return m_content;
	}

	// public Set<INode> getChildren() {
	//
	// Set<INode> children = new HashSet<INode>();
	//
	// if (this.m_tree == null) {
	// return children;
	// } else {
	// Set<IEdge> out_edges = this.m_tree.outgoingEdgesOf(this);
	//
	// for (IEdge edge : out_edges) {
	// children.add(this.m_tree.getEdgeTarget(edge));
	// }
	//
	// return children;
	// }
	// }

	/**
	 * @return the domain
	 */
	public String getDomain() {
		return m_domain;
	}

	public double getID() {
		return this.m_id;
	}

	/**
	 * @return the pathHashValue
	 */
	public int getPathHashValue() {
		return m_pathHashValue;
	}

	public List<String> getRangeExtensions() {
		return this.m_RangeExtensions;
	}

	// public boolean hasChildren() {
	// return this.m_tree == null ? false : this.m_tree.outgoingEdgesOf(this)
	// .size() > 0;
	// }

	/**
	 * @return the sourceExtensions
	 */
	public List<String> getSourceExtensions() {
		return m_SourceExtensions;
	}

	public HashSet<NodeType> getTypes() {
		return m_types;
	}

	/**
	 * @return the m_value
	 */
	public String getValue() {
		return m_value;
	}

	/**
	 * @return the weight
	 */
	public double getWeight() {
		return weight;
	}

	public boolean hasPathHashValue() {
		return m_pathHashValue != Integer.MIN_VALUE;
	}

	public boolean isEndPoint() {
		return m_types.contains(NodeType.ENDPOINT);
	}

	public boolean isInnerNode() {
		return m_types.contains(NodeType.INNER_NODE);
	}

	public boolean isLeave() {
		return m_types.contains(NodeType.LEAVE);
	}

	public boolean isRangeRoot() {
		return m_types.contains(NodeType.RANGE_ROOT);
	}

	public boolean isRoot() {
		return m_types.contains(NodeType.ROOT);
	}

	/**
	 * @param m_content
	 *            the m_content to set
	 */
	public void setContent(NodeContent content) {
		m_content = content;
	}

	/**
	 * @param domain
	 *            the domain to set
	 */
	public void setDomain(String domain) {
		m_domain = domain;
	}

	public void setID(double id) {
		this.m_id = id;
	}

	/**
	 * @param pathHashValue
	 *            the pathHashValue to set
	 */
	public void setPathHashValue(int pathHashValue) {
		m_pathHashValue = pathHashValue;
	}

	public void setRangeExtensions(List<String> extensions) {
		this.m_RangeExtensions = extensions;
	}

	/**
	 * @param sourceExtensions
	 *            the sourceExtensions to set
	 */
	public void setSourceExtensions(List<String> sourceExtensions) {
		m_SourceExtensions = sourceExtensions;
	}

	public void setTypes(HashSet<NodeType> types) {
		this.m_types = types;
	}

	/**
	 * @param m_value
	 *            the m_label to set
	 */
	public void setValue(String value) {
		this.m_value = value;
	}

	/**
	 * @param weight
	 *            the weight to set
	 */
	public void setWeight(double weight) {
		this.weight = weight;
	}

	@Override
	public String toString() {
		return "Node" + this.m_id + " :[Label:" + this.m_value + ", Type:"
				+ this.m_types + ", Content:" + this.m_content
				+ ", Extensions: " + this.m_RangeExtensions + "]";
	}
}
