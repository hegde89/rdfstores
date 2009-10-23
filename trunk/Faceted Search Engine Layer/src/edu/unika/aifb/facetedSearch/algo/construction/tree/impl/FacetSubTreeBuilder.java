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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetSubTreeBuilder implements IBuilder {

	private static Logger s_log = Logger.getLogger(FacetSubTreeBuilder.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private Int2ObjectOpenHashMap<StaticNode> m_paths;

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private FacetIndex m_facetIndex;

	/*
	 * 
	 */
	private HashSet<String> m_parsedSubjects;

	public FacetSubTreeBuilder(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_helper = helper;

		init();
	}

	public boolean build(FacetTree tree, StaticNode node)
			throws DatabaseException, IOException, CacheException,
			StorageException {

		long time1 = System.currentTimeMillis();

		if (node.getFacet().isObjectPropertyBased()) {

			String domain = tree.getDomain();
			Set<StaticNode> newLeaves = new HashSet<StaticNode>();

			if (!(node instanceof FacetValueNode)) {

				Iterator<String> subjIter = m_session.getCache()
						.getSubjects4Node(node).iterator();
				m_helper.clearParsedFacetValues();

				while (subjIter.hasNext()) {

					String subject = subjIter.next();

					if (!m_parsedSubjects.contains(subject)) {

						Iterator<AbstractSingleFacetValue> objIter = m_session
								.getCache().getObjects4StaticNode(node, subject)
								.iterator();

						Iterator<String> sourcesIter = m_session.getCache()
								.getSources4Object(domain, subject).iterator();

						while (objIter.hasNext()) {

							AbstractSingleFacetValue fv = objIter.next();

							/*
							 * update object -> source mapping
							 */

							if (sourcesIter.hasNext()) {

								while (sourcesIter.hasNext()) {
									m_session.getCache()
											.addObject2SourceMapping(domain,
													fv.getValue(),
													sourcesIter.next());
								}
							} else {
								m_session.getCache().addObject2SourceMapping(
										domain, fv.getValue(), subject);
							}

							// /*
							// * update tree
							// */
							//
							// Collection<Node> oldLeaves = m_facetIndex
							// .getLeaves(fv);
							//
							// if (!oldLeaves.isEmpty()) {
							//
							// for (Node leave : oldLeaves) {
							//
							// StaticNode newLeave = m_helper
							// .insertPathAtNode(tree, leave,
							// node, m_paths);
							// newLeaves.add(newLeave);
							//
							// m_cache.updateLeaveGroups(newLeave.getID(),
							// fv.getValue());
							// }
							// } else {

							m_helper.insertFacetValue(tree, node, fv);
							// }
						}

						m_parsedSubjects.add(subject);
					}
				}

				if (!newLeaves.isEmpty()) {

					node.setIsSubTreeRoot(true);
					tree.addLeaves2SubtreeRoot(node.getID(), newLeaves);

					// prune ranges
					// tree = m_helper.pruneRanges(tree, newLeaves);
				}

				long time2 = System.currentTimeMillis();

				s_log.debug("constructed subtree for node '" + node + "' in "
						+ (time2 - time1) + " ms!");

				return true;

			} else {
				return false;
			}
		} else {
			s_log.error("facet " + node.getFacet() + " has invalid facetType!");
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#clean()
	 */
	public void clean() {
		m_paths.clear();
		m_parsedSubjects.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.facetedSearch.algo.construction.tree.IBuilder#close()
	 */
	public void close() {

		clean();
		m_paths = null;
		m_parsedSubjects = null;
	}

	private void init() {

		m_paths = new Int2ObjectOpenHashMap<StaticNode>();
		m_parsedSubjects = new HashSet<String>();

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
}
