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
package edu.unika.aifb.facetedSearch.algo.construction.tree;

import java.io.IOException;
import java.util.Collection;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public interface IFacetTreeBuilder {

	public FacetTree constructSubTree(Collection<String> results, int column,
			StaticNode node, int depth) throws StorageException, IOException,
			DatabaseException;

	public FacetTree constructTree(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException;

}
