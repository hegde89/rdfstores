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
package edu.unika.aifb.facetedSearch.algo.refinement.impl;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.algo.refinement.IRefiner;

/**
 * @author andi
 * 
 */
public class RefinerPool {

	private static SimpleRefiner s_simpleRefiner;
	private static ExtendedRefiner s_extendedRefiner;

	public static IRefiner getRefiner() {

		IRefiner iRefiner;

		switch (FacetedSearchLayerConfig.getRefinementMode()) {

			case FacetEnvironment.RefinementMode.DONT_SELECT_NEW_VARS : {

				if(s_simpleRefiner == null) {
					s_simpleRefiner = new SimpleRefiner();
				}
				
				iRefiner = s_simpleRefiner;
				break;
			}
			case FacetEnvironment.RefinementMode.SELECT_NEW_VARS : {

				if(s_extendedRefiner == null) {
					s_extendedRefiner = new ExtendedRefiner();
				}
				
				iRefiner = s_extendedRefiner;
				break;
			}

			default :
				iRefiner = null;
		}

		return iRefiner;
	}
}
