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
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import edu.unika.aifb.facetedSearch.facets.tree.model.IFacetValueNode;

/**
 * @author andi
 * 
 */
public class FacetValueNode extends StaticNode implements IFacetValueNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4611143426289856264L;

	public FacetValueNode(String value) {
		super(value, NodeType.LEAVE);
	}

	@Override
	public int getCountFV() {
		return 1;
	}

	@Override
	public void setCountFV(int countFV) {
	}

}
