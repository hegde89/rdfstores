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
package edu.unika.aifb.facetedSearch.algo.ranking;

import edu.unika.aifb.facetedSearch.Delegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.IEdge;
import edu.unika.aifb.facetedSearch.facets.tree.model.INode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class RankingDelegator extends Delegator {

	@SuppressWarnings("unused")
	private SearchSession m_session;
	private static RankingDelegator s_instance;

	private RankingDelegator(SearchSession session) {
		m_session = session;
	}

	public static RankingDelegator getInstance(SearchSession session) {
		return s_instance == null ? s_instance = new RankingDelegator(session)
				: s_instance;
	}

	public void doRanking(IEdge edge, INode node){
		
//		double score = 0L;
		
//		TODO
//		
//		edge.setWeight(score);		
	}
	
	public void clean(){
//		TODO
	}

	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.Delegator#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.Delegator#isOpen()
	 */
	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}
}
