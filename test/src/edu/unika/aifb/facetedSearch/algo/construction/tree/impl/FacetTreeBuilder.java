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
import java.util.ArrayList;
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
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.index.db.TransactionalStorageHelperThread;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
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

	/*
	 * 
	 */
	private Int2ObjectOpenHashMap<StaticNode> m_paths;

	/*
	 * Indices
	 */
	private FacetIndex m_facetIndex;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private HashSet<String> m_parsedResources;

	/*
	 * 
	 */
	private long m_time1;
	private long m_time2;

	public FacetTreeBuilder(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_helper = helper;

		init();
	}

	public boolean build(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException,
			CacheException {

		m_time1 = System.currentTimeMillis();

		String domain = results.getColumnName(column);

		FacetTree newTree = new FacetTree();
		newTree.setDomain(domain);

		Set<StaticNode> newLeaves = new HashSet<StaticNode>();
		Iterator<String[]> iter = results.iterator();

		int thread_count = 0;
		ArrayList<TransactionalStorageHelperThread<Double, String>> storageHelpers = new ArrayList<TransactionalStorageHelperThread<Double, String>>();

		while (iter.hasNext()) {

			if (timeOut()) {
				break;
			}

			String resItem = FacetUtils.cleanURI(iter.next()[column]);

			if (!m_parsedResources.contains(resItem)) {

				Collection<Node> oldLeaves = m_facetIndex.getLeaves(resItem);

				for (Node leave : oldLeaves) {

					StaticNode newLeave = m_helper.insertPathAtRoot(newTree,
							leave, m_paths);
					newLeaves.add(newLeave);

					TransactionalStorageHelperThread<Double, String> helperThread = m_session
							.getCache().updateLeaveGroups(newLeave.getID(),
									resItem);

					if (helperThread != null) {
						storageHelpers.add(helperThread);
					}

					thread_count++;

					if (thread_count > FacetEnvironment.DefaultValue.MAX_THREADS) {

						for (TransactionalStorageHelperThread<Double, String> thread : storageHelpers) {

							try {
								thread.join();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}

						storageHelpers.clear();
						thread_count = 0;
					}
				}

				m_parsedResources.add(resItem);
			}
		}

		for (TransactionalStorageHelperThread<Double, String> helperThread : storageHelpers) {

			try {
				helperThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// prune ranges
		newTree = m_helper.pruneRanges(newTree, newLeaves);
		// add leaves for this subtree
		newTree.addLeaves2SubtreeRoot(newTree.getRoot().getID(), newLeaves);

		m_time2 = System.currentTimeMillis();

		s_log.debug("constructed tree for domain '"
				+ results.getColumnName(column) + "' in " + (m_time2 - m_time1)
				+ " ms!");

		m_treeDelegator.storeTree(results.getColumnName(column), newTree);
		m_parsedResources.clear();

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#clean()
	 */
	public void clean() {

		m_paths.clear();
		m_parsedResources.clear();
	}

	public void close() {

		clean();
		m_paths = null;
		m_parsedResources = null;
	}

	private void init() {

		m_treeDelegator = (FacetTreeDelegator) m_session
				.getDelegator(Delegators.TREE);

		m_paths = new Int2ObjectOpenHashMap<StaticNode>();
		m_parsedResources = new HashSet<String>();

		try {

			m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
					IndexName.FACET_INDEX);

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

	private boolean timeOut() {
		return (System.currentTimeMillis() - m_time1) > FacetEnvironment.DefaultValue.MAX_COMPUTATION_TIME;
	}
}