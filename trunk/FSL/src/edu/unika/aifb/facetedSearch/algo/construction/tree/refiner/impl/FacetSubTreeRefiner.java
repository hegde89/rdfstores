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
package edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.impl;

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

import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig.Value;
import edu.unika.aifb.facetedSearch.algo.construction.tree.impl.BuilderHelper;
import edu.unika.aifb.facetedSearch.algo.construction.tree.refiner.IRefiner;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.SingleValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticClusterNode;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetSubTreeRefiner implements IRefiner {

	private static Logger s_log = Logger.getLogger(FacetSubTreeRefiner.class);

	/*
	 * 
	 */
	private SearchSession m_session;
	private FacetIndex m_facetIndex;

	/*
	 * 
	 */
	private BuilderHelper m_helper;

	/*
	 * 
	 */
	private HashSet<String> m_parsedSubjects;
	private Int2ObjectOpenHashMap<StaticClusterNode> m_paths;

	public FacetSubTreeRefiner(SearchSession session, BuilderHelper helper) {

		m_session = session;
		m_helper = helper;

		init();
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

		m_paths = new Int2ObjectOpenHashMap<StaticClusterNode>();
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

	public boolean refine(FacetTree tree, StaticClusterNode node)
			throws DatabaseException, IOException, CacheException,
			StorageException {

		long time1 = System.currentTimeMillis();

		if (node.getCurrentFacet().isObjectPropertyBased()) {

			String domain = tree.getDomain();
			Set<StaticClusterNode> newLeaves = new HashSet<StaticClusterNode>();

			if (!(node instanceof SingleValueNode)) {

				Iterator<String> subjIter = m_session.getCache()
						.getSubjects4Node(node).iterator();
				m_helper.clean();

				while (subjIter.hasNext()) {

					String subject = subjIter.next();

					if (!m_parsedSubjects.contains(subject)) {

						Iterator<AbstractSingleFacetValue> objIter = m_session
								.getCache()
								.getObjects4StaticNode(node, subject)
								.iterator();

						Iterator<String> sourcesIter = m_session.getCache()
								.getSources4Object(domain, subject).iterator();

						while (objIter.hasNext()) {

							AbstractSingleFacetValue fv = objIter.next();

							/*
							 * update object2source mapping
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

							if(FacetedSearchLayerConfig.getRefinementMode().equals(Value.Refinement.MORE_HOP)) {
								
								/*
								 * update tree
								 */
								Collection<Node> oldLeaves = m_facetIndex
										.getLeaves(fv);

								if (!oldLeaves.isEmpty()) {

									for (Node leave : oldLeaves) {

										StaticClusterNode newLeave = m_helper
												.insertPathAtNode(tree, leave,
														node, m_paths);
										newLeaves.add(newLeave);

										/*
										 * update leave group
										 */
										m_session.getCache().updateLeaveGroups(
												newLeave.getID(), fv.getValue());

									}
								}

								/*
								 * insert 'label:localName' for each resource
								 */

								StaticClusterNode newLeave = m_helper
										.insertFacetValueAsResource(tree, node, fv);								
								m_helper.insertFacetValue(tree, newLeave, fv);	
								
								newLeaves.add(newLeave);

								/*
								 * update leave group
								 */
								m_session.getCache().updateLeaveGroups(
										newLeave.getID(), fv.getValue());
								
							} else {
							
								m_helper.insertFacetValue(tree, node, fv);								
							}
						}

						m_parsedSubjects.add(subject);
					}
				}

				if (!newLeaves.isEmpty()) {

					node.setIsSubTreeRoot(true);
					tree.addLeaves2SubtreeRoot(node.getID(), newLeaves);
				}

				long time2 = System.currentTimeMillis();

				s_log.debug("constructed subtree for node '" + node + "' in "
						+ (time2 - time1) + " ms!");

				clean();				
				return true;

			} else {
				return false;
			}
		} else {
			s_log.error("facet " + node.getCurrentFacet()
					+ " has invalid facetType!");
			return false;
		}
	}
}