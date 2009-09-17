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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public interface IStaticNode extends INode {

	// public void addUnsortedObjects(String objects);

	public void addUnsortedObjects(List<String> objects);
	
	public void addSortedObjects(List<String> objects);

	public void addSourceIndivdiual(String ind);

	public int getCountFV();

	public int getCountS();

	public int getHeight();

	public String getName();

	public HashSet<String> getObjects() throws DatabaseException, IOException;

	public int getSize();

	public HashSet<String> getSourceIndivdiuals() throws DatabaseException,
			IOException;

	public void incrementCountFV(int increment);

	public void incrementCountS(int increment);

	public void setCache(SearchSessionCache cache);

	public void setCountFV(int countFV);

	public void setCountS(int countS);

	public void setHeight(int height);

	public void setName(String name);

	public void setSize(int size);

}
