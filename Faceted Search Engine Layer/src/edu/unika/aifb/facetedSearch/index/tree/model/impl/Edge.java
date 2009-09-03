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
package edu.unika.aifb.facetedSearch.index.tree.model.impl;

import org.jgrapht.graph.DefaultEdge;

import edu.unika.aifb.facetedSearch.index.tree.model.IEdge;

/**
 * @author andi
 * 
 */
public class Edge extends DefaultEdge implements IEdge {

	public class EdgeType {

		public static final String SUBPROPERTY_OF = "SubPropertyOf";
		public static final String SUBCLASS_OF = "SubClassOf";
		public static final String HAS_RANGE = "hasRange";

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4884789998330360567L;

	private String m_type;

	public Edge() {
		super();
	}

	public Edge(String type) {
		super();
		this.m_type = type;
	}

	public String getType() {
		return this.m_type;
	}

	public void setType(String type) {
		this.m_type = type;
	}

//	@Override
//	public String toString() {
//		return "Edge: [Type: " + this.m_type + "]";
//	}
}
