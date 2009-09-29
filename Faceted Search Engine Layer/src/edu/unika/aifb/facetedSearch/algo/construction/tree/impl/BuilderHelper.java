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
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.jcs.access.exception.CacheException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge.EdgeType;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class BuilderHelper {

	private static Logger s_log = Logger.getLogger(BuilderHelper.class);

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private SearchSession m_session;
	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	private FacetIndex m_facetIndex;

	public BuilderHelper(SearchSession session) {

		m_session = session;
		m_cache = session.getCache();

		try {

			m_facetIndex = (FacetIndex) session.getStore().getIndex(
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

	public void attachRDFTypes(FacetTree tree, Set<StaticNode> rdfprop_eps)
			throws DatabaseException, IOException {

		for (StaticNode node : rdfprop_eps) {
			insertFacetValues(tree, node, node.getObjects());
		}
	}

	public void insertFacetValues(FacetTree tree, StaticNode node,
			Collection<? extends AbstractFacetValue> values) {

		Iterator<? extends AbstractFacetValue> valueIter = values.iterator();

		while (valueIter.hasNext()) {

			AbstractFacetValue fv = valueIter.next();
			FacetValueNode fvNode = new FacetValueNode(fv.getValue());
			fvNode.setDomain(node.getDomain());
			fvNode.setCache(m_cache);
			fvNode.setDepth(node.getDepth() + 1);

		}
	}

	public StaticNode insertPathAtNode(FacetTree newTree,
			FacetTree indexedTree, double leaveId, StaticNode node,
			Int2ObjectOpenHashMap<StaticNode> paths) throws DatabaseException,
			IOException, CacheException {

		Node leave = indexedTree.getVertex(leaveId);

		if (!paths.containsKey(leave.getPathHashValue())) {

			StaticNode pos4insertion = node;
			Stack<Edge> edges2insert = new Stack<Edge>();

			Queue<Edge> path2root = m_cache.getAncestorPath2Root(indexedTree,
					leaveId);

			while (!path2root.isEmpty()) {

				Edge currentEdge = path2root.poll();
				int currentPathHash = indexedTree.getEdgeTarget(currentEdge)
						.getPathHashValue();

				if (!paths.containsKey(currentPathHash)) {
					edges2insert.add(currentEdge);
				} else {
					pos4insertion = paths.get(currentPathHash);
					break;
				}
			}

			if (!edges2insert.isEmpty()) {

				String pathPrefix = pos4insertion.getPath();
				String pathDelta = "";

				while (!edges2insert.isEmpty()) {

					Edge edge2insert = edges2insert.pop();
					Node node2copy = indexedTree.getEdgeTarget(edge2insert);

					StaticNode newNode = new StaticNode();
					newNode.setContent(node2copy.getContent());
					newNode.setFacet(node2copy.getFacet());
					newNode.setValue(node2copy.getValue());
					newNode.setType(node2copy.getType());
					newNode.setDomain(newTree.getDomain());
					newNode.setCache(m_cache);
					newNode.setID(node2copy.getID());
					newNode.setDepth(pos4insertion.getDepth() + 1);

					String path = pathPrefix + pathDelta + newNode.getValue();
					newNode.setPath(path);
					newNode.setPathHashValue(path.hashCode());
					paths.put(path.hashCode(), newNode);
					pathDelta = pathDelta + newNode.getValue();

					newTree.addVertex(newNode);

					Edge edge = newTree.addEdge(pos4insertion, newNode);
					edge.setType(edge2insert.getType());

					pos4insertion = newNode;
				}

				newTree.addEndPoint(FacetUtils
						.getEndPointType4Node(pos4insertion), pos4insertion);
			}

			return pos4insertion;

		} else {

			return paths.get(leave.getPathHashValue());
		}
	}

	public StaticNode insertPathAtRoot(FacetTree newTree,
			FacetTree indexedTree, double leaveId,
			Int2ObjectOpenHashMap<StaticNode> paths) throws DatabaseException,
			IOException, CacheException {

		return insertPathAtNode(newTree, indexedTree, leaveId, newTree
				.getRoot(), paths);
	}

	public FacetTree pruneRanges(FacetTree tree, Set<StaticNode> leaves)
			throws DatabaseException {

		Iterator<StaticNode> iter = leaves.iterator();

		while (iter.hasNext()) {

			StaticNode currentNode = iter.next();
			boolean reachedRangeRoot = false;

			// walk to root

			while (!reachedRangeRoot) {

				Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.isRangeRoot()) {

					reachedRangeRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = tree.getEdgeSource(edge2father);

					if (father.isRangeRoot()) {

						reachedRangeRoot = true;

					} else {

						if ((tree.outgoingEdgesOf(father).size() == 1)
								&& (!tree.getEndPoints().contains(
										father.getID()))) {

							Edge edge2fathersfather = tree.incomingEdgesOf(
									father).iterator().next();

							StaticNode fathersfather = (StaticNode) tree
									.getEdgeSource(edge2fathersfather);
							fathersfather
									.setHeight(currentNode.getHeight() + 1);

							Edge newEdge = tree.addEdge(fathersfather,
									currentNode);
							newEdge.setType(EdgeType.SUBCLASS_OF);

							tree.removeEdge(father, currentNode);
							tree.removeEdge(fathersfather, father);

							tree.removeVertex(father);

						} else {

							currentNode = (StaticNode) father;
						}
					}
				} else {
					s_log.error("tree structure is not correct: " + tree);
					break;
				}
			}
		}

		return tree;
	}

	public void updateNodeCounts(FacetTree newTree, String resItem,
			String sourceExt, StaticNode leave) throws CacheException,
			EnvironmentLockedException, DatabaseException, IOException {

		Queue<Edge> path2RangeRoot = m_cache.getAncestorPath2RangeRoot(newTree,
				leave.getID());

		Collection<AbstractSingleFacetValue> objects = m_facetIndex.getObjects(
				leave, resItem);

		if (objects.size() == 0) {
			s_log.error("no objects found for leave '" + leave + "' and ind '"
					+ resItem + "'!");
		}

		if (!path2RangeRoot.isEmpty()) {

			if (leave.getFacet().getType() != FacetType.DATAPROPERTY_BASED) {

				StaticNode last_node = null;
				StaticNode current_node = null;

				while (!path2RangeRoot.isEmpty()) {

					Edge edge = path2RangeRoot.poll();

					current_node = (StaticNode) newTree.getEdgeTarget(edge);
					current_node.addSourceIndivdiual(resItem);
					current_node.addUnsortedObjects(objects, resItem);

					last_node = (StaticNode) newTree.getEdgeSource(edge);
				}

				last_node.addSourceIndivdiual(resItem);
				last_node.addUnsortedObjects(objects, resItem);

			} else {
				s_log.error("should not be here ... leave '" + leave + "'");
			}
		} else {

			leave.addSourceIndivdiual(resItem);

			if (leave.getFacet().getType() == FacetType.DATAPROPERTY_BASED) {
				leave.addSortedObjects(objects, resItem);
			}
			// object property or rdf property based
			else {
				leave.addUnsortedObjects(objects, resItem);
			}
		}
	}
}
