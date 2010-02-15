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
package edu.unika.aifb.facetedSearch.facets.converter.facet2tree;

import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;

/**
 * @author andi
 * 
 */
public class Facet2TreeModelConverter extends AbstractConverter {

	private static Facet2TreeModelConverter s_instance;

	public static Facet2TreeModelConverter getInstance() {
		return s_instance == null
				? s_instance = new Facet2TreeModelConverter()
				: s_instance;
	}

	private Facet2TreeModelConverter() {
	}

	public Node facet2Node(SearchSession session, Facet facet) {
		return getNode(session, facet.getDomain(), facet.getNodeId());
	}

	public Node facetValue2Node(SearchSession session, AbstractFacetValue fv) {
		return getNode(session, fv.getDomain(), fv.getNodeId());
	}

	private Node getNode(SearchSession session, String domain, double nodeID) {
		return ((FacetTreeDelegator) session.getDelegator(Delegators.TREE))
				.getNode(domain, nodeID);
	}
}