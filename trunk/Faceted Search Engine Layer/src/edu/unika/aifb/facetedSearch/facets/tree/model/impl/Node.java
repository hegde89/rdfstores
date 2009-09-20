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

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.api.model.impl.Facet;
import edu.unika.aifb.facetedSearch.api.model.impl.Facet.FacetType;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;
import edu.unika.aifb.facetedSearch.util.FacetUtils;

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
		// , ENDPOINT
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

	// public Node(String value, NodeType type) {
	//
	// m_value = value;
	// m_type = type;
	// m_id = (new Random()).nextGaussian();
	// m_RangeExtensions = new ArrayList<String>();
	// m_SourceExtensions = new ArrayList<String>();
	// // setPathHashValue(pathHashValue);
	// }

	public Node(String value, NodeContent content) {

		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();
		// setPathHashValue(pathHashValue);
	}

	public Node(String value, NodeType type) {

		// m_types = new HashSet<NodeType>();
		m_type = type;
		m_value = value;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();
		// setPathHashValue(pathHashValue);
	}

	public Node(String value, NodeType type, NodeContent content) {

		m_type = type;
		m_value = value;
		m_content = content;
		m_id = (new Random()).nextGaussian();
		m_RangeExtensions = new HashSet<String>();
		m_SourceExtensions = new HashSet<String>();
		// setPathHashValue(pathHashValue);
	}

	public void addRangeExtension(String extension) {
		m_RangeExtensions.add(extension);
	}

	public void addRangeExtensions(Collection<String> extensions) {
		m_RangeExtensions.addAll(extensions);
	}

	public void addRangeExtensions(String extensions) {
		m_RangeExtensions.addAll(FacetUtils.string2List(extensions));
	}

	public void addSourceExtension(String extension) {
		m_SourceExtensions.add(extension);
	}

	public void addSourceExtensions(Collection<String> extensions) {
		m_SourceExtensions.addAll(extensions);
	}

	public void addSourceExtensions(String extensions) {
		m_SourceExtensions.addAll(FacetUtils.string2List(extensions));
	}

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

	// public void addType(NodeType type) {
	// m_types.add(type);
	// }
	//
	// public void addTypes(Collection<NodeType> collection) {
	// m_types.addAll(collection);
	// }

	// /**
	// * @return the cache
	// */
	// public SearchSessionCache getCache() {
	// return m_cache;
	// }

	public String getDomain() {
		return m_domain;
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

	public Facet getFacet() {
		return m_facet;
	}

	public double getID() {
		return this.m_id;
	}

	/**
	 * @return the path
	 */
	public String getPath() {
		return m_path;
	}

	/**
	 * @return the pathHashValue
	 */
	public int getPathHashValue() {
		return m_pathHashValue;
	}

	public HashSet<String> getRangeExtensions() {
		return m_RangeExtensions;
	}

	// public boolean hasChildren() {
	// return this.m_tree == null ? false : this.m_tree.outgoingEdgesOf(this)
	// .size() > 0;
	// }

	/**
	 * @return the sourceExtensions
	 */
	public HashSet<String> getSourceExtensions() {
		return m_SourceExtensions;
	}

	public NodeType getType() {
		return m_type;
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

	// public boolean isEndPoint() {
	// return m_types.contains(NodeType.ENDPOINT);
	// }

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

	// public void removeType(NodeType type) {
	// m_types.remove(type);
	// }

	public Facet makeFacet(String uri, FacetType ftype, DataType dtype) {
		return new Facet(uri, ftype, dtype);
	}

	// /**
	// * @param cache
	// * the cache to set
	// */
	// public void setCache(SearchSessionCache cache) {
	// m_cache = cache;
	// }

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

	/**
	 * @param facet
	 *            the facet to set
	 */
	public void setFacet(Facet facet) {
		m_facet = facet;
	}

	public void setID(double id) {
		this.m_id = id;
	}

	/**
	 * @param path
	 *            the path to set
	 */
	public void setPath(String path) {
		m_path = path;
	}

	/**
	 * @param pathHashValue
	 *            the pathHashValue to set
	 */
	public void setPathHashValue(int pathHashValue) {
		m_pathHashValue = pathHashValue;
	}

	public void setRangeExtensions(HashSet<String> extensions) {
		this.m_RangeExtensions = extensions;
	}

	/**
	 * @param sourceExtensions
	 *            the sourceExtensions to set
	 */
	public void setSourceExtensions(HashSet<String> sourceExtensions) {
		m_SourceExtensions = sourceExtensions;
	}

	public void setType(NodeType type) {
		m_type = type;
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
		this.m_weight = weight;
	}

	@Override
	public String toString() {
		return "Node" + this.m_id + " :[Label:" + this.m_value + ", Type:"
				+ this.m_type + ", Content:" + this.m_content
				+ ", Extensions: " + this.m_RangeExtensions + "]";
	}
}
