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
package edu.unika.aifb.graphindex.facets.model.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.unika.aifb.graphindex.facets.model.INode;

/**
 * @author andi
 * 
 */
public class Node implements INode {

	public enum NodeContent {
		DATA_PROPERTY, OBJECT_PROPERTY, CLASS, TYPE_PROPERTY
	}

	public enum NodeType {
		LEAVE, ROOT, INNER_NODE, RANGE_TOP, ENDPOINT
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1484769190480836362L;

	private String m_value;
	private NodeType m_type;
	private NodeContent m_content;
	private List<String> m_extensions;
	private double m_id;

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

	public Node(String value, NodeContent content) {

		this.m_value = value;
		this.m_content = content;
		this.m_id = (new Random()).nextGaussian();
		this.m_extensions = new ArrayList<String>();
	}

	public Node(String value, NodeType type) {

		this.m_type = type;
		this.m_value = value;
		this.m_id = (new Random()).nextGaussian();
		this.m_extensions = new ArrayList<String>();
	}

	public Node(String value, NodeType type, NodeContent content) {

		this.m_type = type;
		this.m_value = value;
		this.m_content = content;
		this.m_id = (new Random()).nextGaussian();
		this.m_extensions = new ArrayList<String>();
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

	public void addRangeExtension(String extension) {
		this.m_extensions.add(extension);
	}

	public void addRangeExtensions(List<String> extensions) {
		this.m_extensions.addAll(extensions);
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
		return this.m_content;
	}

	// public boolean hasChildren() {
	// return this.m_tree == null ? false : this.m_tree.outgoingEdgesOf(this)
	// .size() > 0;
	// }

	public double getID() {
		return this.m_id;
	}

	/**
	 * @return the m_value
	 */
	public String getValue() {
		return this.m_value;
	}

	public List<String> getRangeExtensions() {
		return this.m_extensions;
	}

	public NodeType getType() {
		return this.m_type;
	}

	/**
	 * @param m_content
	 *            the m_content to set
	 */
	public void setContent(NodeContent content) {
		this.m_content = content;
	}

	public void setID(double id) {
		this.m_id = id;
	}

	/**
	 * @param m_value
	 *            the m_label to set
	 */
	public void setValue(String value) {
		this.m_value = value;
	}

	public void setRangeExtensions(List<String> extensions) {
		this.m_extensions = extensions;
	}

	public void setType(NodeType type) {
		this.m_type = type;
	}

	@Override
	public String toString() {
		return "Node" + this.m_id + " :[Label:" + this.m_value + ", Type:"
				+ this.m_type + ", Content:" + this.m_content
				+ ", Extensions: " + this.m_extensions + "]";
	}
}
