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

	public static Facet2TreeModelConverter getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new Facet2TreeModelConverter(
				session) : s_instance;
	}

	private SearchSession m_session;
	private FacetTreeDelegator m_treeDelegator;

	private Facet2TreeModelConverter(SearchSession session) {
		m_session = session;
		init();
	}

	public Node facet2Node(Facet facet) {
		return getNode(facet.getDomain(), facet.getNodeId());
	}

	public Node facetValue2Node(AbstractFacetValue fv) {
		return getNode(fv.getDomain(), fv.getNodeId());
	}

	private Node getNode(String domain, double nodeID) {

		// m_treeDelegator.get

		return null;
	}

	private void init() {
		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
	}
}
