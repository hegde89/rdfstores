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
package edu.unika.aifb.facetedSearch.algo.construction;

import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 *
 */
public class ConstructionDelegator {

	private SearchSession m_session;
	private static ConstructionDelegator s_instance;
	
	
	private ConstructionDelegator(SearchSession session){
		m_session = session;
	}
	
	public static ConstructionDelegator getInstance(SearchSession session){
		return s_instance == null ? s_instance = new ConstructionDelegator(session) : s_instance;
	}
	
	public void clean(){
		
	}
	
	public void doFacetConstruction(Table<String> results){
		
		FacetTreeDelegator facetTreeDelegator = m_session.getFacetTreeDelegator();
		facetTreeDelegator.clean();
		
//		TODO
		
		
	}
	
}
