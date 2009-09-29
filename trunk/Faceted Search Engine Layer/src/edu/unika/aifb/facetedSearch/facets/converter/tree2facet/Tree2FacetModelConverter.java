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
package edu.unika.aifb.facetedSearch.facets.converter.tree2facet;

import java.util.List;

import edu.unika.aifb.facetedSearch.facets.converter.AbstractConverter;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;

/**
 * @author andi
 * 
 */
public class Tree2FacetModelConverter extends AbstractConverter {

	private static Tree2FacetModelConverter s_instance;

	public static Tree2FacetModelConverter getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new Tree2FacetModelConverter(
				session) : s_instance;
	}

	private SearchSession m_session;
	private FacetTreeDelegator m_treeDelegator;

	private Tree2FacetModelConverter(SearchSession session) {
		m_session = session;
		init();
	}

	private void init() {
		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);
	}

	public List<AbstractFacetValue> nodeList2facetValueList(List<Node> nodeList) {

		// TODO

		return null;
	}
}
