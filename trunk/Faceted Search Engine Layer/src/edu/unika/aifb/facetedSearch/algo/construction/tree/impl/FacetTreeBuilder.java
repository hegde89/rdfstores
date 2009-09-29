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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetTreeBuilder implements IBuilder {

	private static Logger s_log = Logger.getLogger(FacetTreeBuilder.class);

	/*
	 * 
	 */
	private SearchSession m_session;
	@SuppressWarnings("unused")
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private Int2ObjectOpenHashMap<FacetTree> m_indexedTrees;
	private Int2ObjectOpenHashMap<StaticNode> m_paths;

	/*
	 * Indices
	 */
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	public FacetTreeBuilder(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_helper = helper;
		m_cache = session.getCache();

		m_treeDelegator = (FacetTreeDelegator) session
				.getDelegator(Delegators.TREE);

		init();
	}

	public boolean build(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException,
			CacheException {

		long time1 = System.currentTimeMillis();

		FacetTree newTree = new FacetTree();
		newTree.setDomain(results.getColumnName(column));

		Set<StaticNode> newLeaves = new HashSet<StaticNode>();
		Iterator<String[]> iter = results.iterator();

		while (iter.hasNext()) {

			FacetTree currentIndexedTree;

			String resItem = iter.next()[column];
			String sourceExtension = m_structureIndex.getExtension(resItem);

			if (!m_indexedTrees.containsKey(sourceExtension)) {

				FacetTree indexedTree = m_facetIndex.getTree(sourceExtension);
				m_indexedTrees.put(sourceExtension.hashCode(), indexedTree);

				currentIndexedTree = indexedTree;

			} else {

				currentIndexedTree = m_indexedTrees.get(sourceExtension);
			}

			Collection<Double> oldLeaves = m_facetIndex.getLeaves(
					sourceExtension, resItem);

			for (double leave : oldLeaves) {

				StaticNode newLeave = m_helper.insertPathAtRoot(newTree,
						currentIndexedTree, leave, m_paths);
				newLeaves.add(newLeave);

				m_helper.updateNodeCounts(newTree, resItem, sourceExtension,
						newLeave);

			}
		}

		// prune ranges
		newTree = m_helper.pruneRanges(newTree, newLeaves);

		m_helper.attachRDFTypes(newTree, newTree
				.getEndPoints(FacetEnvironment.EndPointType.RDF_PROPERTY));

		long time2 = System.currentTimeMillis();

		s_log.debug("constructed tree for domain '"
				+ results.getColumnName(column) + "' in " + (time2 - time1)
				+ " ms!");

		m_treeDelegator.storeTree(results.getColumnName(column), newTree);
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#clean()
	 */
	public void clean() {

		m_indexedTrees.clear();
		m_paths.clear();
	}

	public void close() {

		clean();
		m_indexedTrees = null;
		m_paths = null;

	}

	private void init() {

		m_indexedTrees = new Int2ObjectOpenHashMap<FacetTree>();
		m_paths = new Int2ObjectOpenHashMap<StaticNode>();

		try {

			m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
					IndexName.FACET_INDEX);
			m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
					IndexName.STRUCTURE_INDEX);

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}
}