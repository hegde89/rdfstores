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
package edu.unika.aifb.facetedSearch.algo.construction.tree.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetTreeBuilder implements IFacetTreeBuilder {

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	// indices
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	private HashSet<String> m_SourceExtension;
	private HashMap<String, FacetTree> m_trees;

	public FacetTreeBuilder(SearchSession session) throws IOException,
			EnvironmentLockedException, DatabaseException, StorageException {

		m_session = session;
		m_cache = m_session.getCache();
		m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
				IndexName.FACET_INDEX);
		m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
				IndexName.STRUCTURE_INDEX);

		m_SourceExtension = new HashSet<String>();
		m_trees = new HashMap<String, FacetTree>();

	}

	public FacetTree contruct(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException {

		FacetTree tree = new FacetTree();
		Iterator<String[]> iter = results.iterator();

		while (iter.hasNext()) {

			FacetTree currentStoredTree;

			String resItem = iter.next()[column];
			// TODO: performance for this lookup?
			String extension = m_structureIndex.getExtension(resItem);

			if (!m_SourceExtension.contains(extension)) {

				// add extenison and load tree into RAM
				m_SourceExtension.add(extension);

				FacetTree storedTree = m_facetIndex.getFacetTree(extension);
				m_trees.put(extension, storedTree);

				currentStoredTree = storedTree;

			} else {
				currentStoredTree = m_trees.get(extension);
			}

			HashSet<Node> leaves = m_facetIndex.getLeaves(extension, resItem);
			
			for (Node leave : leaves) {

				int pathHash = leave.getPathHashValue();
				
			}
		}

		return tree;
	}
}
