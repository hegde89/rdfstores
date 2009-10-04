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
package edu.unika.aifb.facetedSearch.facets.tree.model;

import java.util.List;

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;

/**
 * @author andi
 * 
 */
public interface IDynamicNode extends IStaticNode {

	public String getLeftBorder();

	public List<AbstractSingleFacetValue> getLiterals();

	public String getRightBorder();

	public void setLeftBorder(String leftValue);

	public void setLiterals(List<AbstractSingleFacetValue> lits);

	public void setRightBorder(String rightValue);

}
