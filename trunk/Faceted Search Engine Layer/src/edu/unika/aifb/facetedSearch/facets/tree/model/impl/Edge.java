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

import java.io.Serializable;

import org.jgrapht.graph.DefaultEdge;

import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.facets.tree.model.IEdge;

/**
 * @author andi
 * 
 */
public class Edge extends DefaultEdge implements IEdge, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1972985962703565730L;

	/*
	 * 
	 */
	private int m_type;

	/*
	 * 
	 */
	private Node m_source;
	private Node m_target;

	public Edge() {
		super();
	}

	public Edge(int type) {
		super();
		m_type = type;
	}

	public Node getSource() {
		return m_source;
	}

	public Node getTarget() {
		return m_target;
	}

	public int getType() {
		return m_type;
	}

	public void setSource(Node source) {
		m_source = source;
	}

	public void setTarget(Node target) {
		m_target = target;
	}

	public void setType(int type) {
		m_type = type;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgrapht.graph.DefaultEdge#toString()
	 */
	@Override
	public String toString() {
		return "[" + m_source + " -> " + m_target + " // type: "
				+ type2String() + "]";
	}

	private String type2String() {

		switch (m_type) {
			case EdgeType.CONTAINS : {
				return "contains";
			}
			case EdgeType.HAS_RANGE : {
				return "has_range";
			}
			case EdgeType.SUBCLASS_OF : {
				return "subclass_of";
			}
			case EdgeType.SUBPROPERTY_OF : {
				return "subproperty_of";
			}
			default :
				return "not valid!";
		}
	}
}
