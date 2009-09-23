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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public interface IStaticNode extends INode {

	public void addSortedObjects(HashSet<String> objects, String source);

	public void addSourceIndivdiual(String ind);

	public void addUnsortedObjects(HashSet<String> objects, String source);

	public int getCountFV();

	public int getCountS();

	public int getCountS4Object(String source);

	// public void addUnsortedObjects(String objects);

	public int getCountS4Objects(Collection<String> sources);

	public int getDepth();

	public int getHeight();

	// public HashMap<String, HashSet<Integer>> getLiteralSources();
	//
	// public HashSet<Integer> getLiteralSources(String lit);

	public String getName();

	public HashSet<String> getObjects() throws DatabaseException, IOException;

	public int getSize();

	public List<String> getSortedLiterals();

	public HashSet<String> getSourceIndivdiuals() throws DatabaseException,
			IOException;

	public void incrementCountFV(int increment);

	public void incrementCountS(int increment);

	public void setCache(SearchSessionCache cache);

	public void setCountFV(int countFV);

	public void setCountS(int countS);

	public void setDepth(int depth);

	public void setHeight(int height);

	// public void setLiteralCounts(HashMap<String, HashSet<Integer>>
	// literalCounts);

	public void setName(String name);

	public void setSize(int size);

	public void setSortedLiterals(List<String> sortedLiterals);

}
