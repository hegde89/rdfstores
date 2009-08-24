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
package edu.unika.aifb.facetedSearch.api.model.impl;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 * 
 */
public class IndividualFactorty {

	private SearchSession m_session;

	public IndividualFactorty(SearchSession session) {
		this.m_session = session;
	}

	public IIndividual makeIndividual(String value) {
		return new Individual(this.m_session, value);
	}

}
