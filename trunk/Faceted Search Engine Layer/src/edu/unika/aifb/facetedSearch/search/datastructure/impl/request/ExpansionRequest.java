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
package edu.unika.aifb.facetedSearch.search.datastructure.impl.request;

import edu.unika.aifb.graphindex.query.QNode;


/**
 * @author andi
 * 
 */
public class ExpansionRequest extends AbstractFacetRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6622448083241709767L;
	
	private QNode m_qNode;

	public ExpansionRequest() {
		super("expansionRequest");
	}

	public ExpansionRequest(String name) {
		super(name);
	}

	public void setQNode(QNode qNode) {
		m_qNode = qNode;
	}

	public QNode getQNode() {
		return m_qNode;
	}
}
