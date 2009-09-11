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

import java.util.Map;

import edu.unika.aifb.facetedSearch.api.model.IIndividual;

/**
 * @author andi
 * 
 */
public interface IFacetValueCluster extends INode{
	
	public Map<IIndividual, Integer> getSources();
	
	public int getSize();
		
	public int getHeight();
	
	public void setHeight(int height);
	
	public void setSize(int size);
		
	
	public String getName();
	
	public int getCountFV();
	
	public int getCountS();
	
	
	public void setName(String name);	
	
	public void setCountFV(int countFV);
	
	public void setCountS(int countS);

}
