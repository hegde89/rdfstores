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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.ComparatorPool;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.DistanceComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.metric.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.api.model.impl.Facet.FacetType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge.EdgeType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree.EndPointType;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.ClearType;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.IndexName;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetTreeBuilder implements IFacetTreeBuilder {

	private static Logger s_log = Logger.getLogger(FacetTreeBuilder.class);

	private ComparatorPool m_compPool;

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	private HashMap<String, FacetTree> m_indexedTrees;
	private HashMap<Integer, StaticNode> m_paths;

	/*
	 * Indices
	 */
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	public FacetTreeBuilder(SearchSession session) throws IOException,
			EnvironmentLockedException, DatabaseException, StorageException {

		m_session = session;
		m_cache = session.getCache();

		m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
				IndexName.FACET_INDEX);
		m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
				IndexName.STRUCTURE_INDEX);

		m_indexedTrees = new HashMap<String, FacetTree>();
		m_paths = new HashMap<Integer, StaticNode>();
		m_compPool = ComparatorPool.getInstance(m_cache);

	}

	private void clear() {

		m_indexedTrees.clear();
		m_paths.clear();

	}

	public void close() {

		clear();
		m_indexedTrees = null;
		m_paths = null;

	}

	public FacetTree constructSubTree(Collection<String> results, int column,
			StaticNode node, int depth) throws StorageException, IOException,
			DatabaseException {

		clear();

		long time1 = System.currentTimeMillis();

		if (node.getFacet().isObjectPropertyBased()) {

			FacetTree subTree = new FacetTree();
			subTree.setDomain(node.getDomain());

			Set<StaticNode> newLeaves = new HashSet<StaticNode>();
			Iterator<String> indIter = node.getSourceIndivdiuals().iterator();

			while (indIter.hasNext()) {

				String ind = indIter.next();
				String sourceExtension = m_structureIndex.getExtension(ind);

				FacetTree currentIndexedTree;

				if (!m_indexedTrees.containsKey(sourceExtension)) {

					FacetTree indexedTree = m_facetIndex
							.getTree(sourceExtension);
					m_indexedTrees.put(sourceExtension, indexedTree);

					currentIndexedTree = indexedTree;

				} else {

					currentIndexedTree = m_indexedTrees.get(sourceExtension);

				}

				HashSet<Double> oldLeaves = m_facetIndex.getLeaves(
						sourceExtension, ind);

				for (double leave : oldLeaves) {

					StaticNode newLeave = insertPathAtRoot(subTree,
							currentIndexedTree, leave);
					newLeaves.add(newLeave);

					updateNodes(subTree, ind, sourceExtension, newLeave);

				}
			}

			// prune ranges
			subTree = pruneRanges4Leaves(subTree, newLeaves);

			doClustering(subTree, depth, subTree
					.getEndPoints(EndPointType.DATA_PROPERTY));
			doConstructSubtrees(subTree, depth, subTree
					.getEndPoints(EndPointType.OBJECT_PROPERTY));
			doAttachRDFTypes(subTree, subTree
					.getEndPoints(EndPointType.RDF_PROPERTY));

			long time2 = System.currentTimeMillis();

			s_log.debug("constructed subtree for node '" + node + "' in "
					+ (time2 - time1) + " ms!");

			return subTree;

		} else if (node.getFacet().isDataPropertyBased()) {

			// TODO

			return null;

		} else {
			s_log.error("facet " + node.getFacet() + " has invalid facetType!");
			return null;
		}
	}

	public FacetTree constructTree(Table<String> results, int column)
			throws StorageException, IOException, DatabaseException {

		clear();

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
				m_indexedTrees.put(sourceExtension, indexedTree);

				currentIndexedTree = indexedTree;

			} else {

				currentIndexedTree = m_indexedTrees.get(sourceExtension);
			}

			HashSet<Double> oldLeaves = m_facetIndex.getLeaves(sourceExtension,
					resItem);

			for (double leave : oldLeaves) {

				StaticNode newLeave = insertPathAtRoot(newTree,
						currentIndexedTree, leave);
				newLeaves.add(newLeave);

				updateNodes(newTree, resItem, sourceExtension, newLeave);

			}
		}

		// prune ranges
		newTree = pruneRanges4Leaves(newTree, newLeaves);

		doClustering(newTree, FacetEnvironment.DefaultValue.DEPTH_K, newTree
				.getEndPoints(EndPointType.DATA_PROPERTY));
		doAttachRDFTypes(newTree, newTree
				.getEndPoints(EndPointType.RDF_PROPERTY));
		doConstructSubtrees(newTree, FacetEnvironment.DefaultValue.DEPTH_K,
				newTree.getEndPoints(EndPointType.OBJECT_PROPERTY));

		long time2 = System.currentTimeMillis();

		s_log.debug("constructed tree for domain '"
				+ results.getColumnName(column) + "' in " + (time2 - time1)
				+ " ms!");

		return newTree;
	}

	private void doAttachRDFTypes(FacetTree tree, Set<Node> rdfprop_eps) {
		// TODO
	}

	@SuppressWarnings("unchecked")
	private void doClustering(FacetTree tree, int depth_k,
			Set<Node> dataprop_eps) throws DatabaseException {

		DistanceComparator disComp = new DistanceComparator();
		Iterator<Node> epIter = dataprop_eps.iterator();

		while (epIter.hasNext()) {

			m_cache.clear(ClearType.LITERALS);

			StaticNode ep = (StaticNode) epIter.next();
			DataType datatype = ep.getFacet().getDataType() == null ? FacetEnvironment.DataType.STRING
					: ep.getFacet().getDataType();

			IDistanceMetric metric = DistanceMetricPool.getMetric(datatype);
			List<String> lits = ep.getSortedLiterals();

			/*
			 * merge sort: O(nlogn)
			 */
			Collections.sort(lits, m_compPool.getComparator(datatype));

			if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

				/*
				 * each insert has O(logn)
				 */
				PriorityQueue<ClusterDistance> distanceQueue = new PriorityQueue<ClusterDistance>(
						ep.getSortedLiterals().size() - 1, disComp);
				Iterator<String> litIter = ep.getSortedLiterals().iterator();

				HashSet<Integer> current_leftSources = new HashSet<Integer>();
				int current_leftCountS = 0;
				int current_leftCountFV = 0;

				ArrayList<ClusterDistance> current_leftDistances = new ArrayList<ClusterDistance>();

				while (litIter.hasNext()) {

					String current_left = litIter.next();
					current_leftSources.addAll(ep
							.getLiteralSources(current_left));

					current_leftCountS = current_leftSources.size();
					current_leftCountFV += 1;

					if (litIter.hasNext()) {

						String current_right = litIter.next();

						BigDecimal distanceValue = metric.getDistance(m_cache
								.getParsedLiteral(current_left.hashCode()),
								m_cache.getParsedLiteral(current_right
										.hashCode()));

						ClusterDistance clusterDistance = new ClusterDistance(
								current_left, current_right);

						clusterDistance.setLeftDistances(current_leftDistances);
						clusterDistance.setValue(distanceValue);
						clusterDistance.setLeftCountFV(current_leftCountFV);
						clusterDistance.setLeftCountS(current_leftCountS);

						boolean success = distanceQueue.offer(clusterDistance);

						if (!success) {

							s_log.error("could not insert distance '"
									+ clusterDistance + "' ");

						} else {

							current_leftDistances.add(clusterDistance);

						}
					}
				}
			} else {

			}
		}
	}

	private void doConstructSubtrees(FacetTree tree, int depth_k,
			Set<Node> objectprop_eps) {

	}

	private EndPointType getEndPointType(Node endpoint) {

		EndPointType epType;

		if (endpoint.getFacet().getType() == FacetType.DATAPROPERTY_BASED) {
			epType = EndPointType.DATA_PROPERTY;
		} else if (endpoint.getFacet().getType() == FacetType.OBJECT_PROPERTY_BASED) {
			epType = EndPointType.OBJECT_PROPERTY;
		} else {
			epType = EndPointType.RDF_PROPERTY;
		}

		return epType;
	}

	private StaticNode insertPathAtNode(FacetTree newTree,
			FacetTree indexedTree, double leaveId, StaticNode node)
			throws DatabaseException, IOException {

		Node leave = indexedTree.getNodeById(leaveId);

		if (!m_paths.containsKey(leave.getPathHashValue())) {

			StaticNode pos4insertion = node;
			Stack<Edge> edges2insert = new Stack<Edge>();

			Queue<Edge> path2root = m_cache.getAncestorPath2Root(indexedTree,
					leaveId);

			while (!path2root.isEmpty()) {

				Edge currentEdge = path2root.poll();
				int currentPathHash = indexedTree.getEdgeTarget(currentEdge)
						.getPathHashValue();

				if (!m_paths.containsKey(currentPathHash)) {
					edges2insert.add(currentEdge);
				} else {
					pos4insertion = m_paths.get(currentPathHash);
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
					m_paths.put(path.hashCode(), newNode);
					pathDelta = pathDelta + newNode.getValue();

					newTree.addVertex(newNode);

					Edge edge = newTree.addEdge(pos4insertion, newNode);
					edge.setType(edge2insert.getType());

					pos4insertion = newNode;
				}

				newTree.addEndPoint(getEndPointType(pos4insertion),
						pos4insertion);
			}

			return pos4insertion;

		} else {

			return m_paths.get(leave.getPathHashValue());
		}
	}

	private StaticNode insertPathAtRoot(FacetTree newTree,
			FacetTree indexedTree, double leaveId) throws DatabaseException,
			IOException {

		return insertPathAtNode(newTree, indexedTree, leaveId, newTree
				.getRoot());
	}

	private void insertFacetValues(FacetTree tree, StaticNode node, Collection<String> values) {

		Iterator<String> valueIter = values.iterator();

		while (valueIter.hasNext()) {

			String valueStrg = valueIter.next();
			FacetValue valueNode = new FacetValue(valueStrg);
			
		}
	}

	private FacetTree pruneRanges4Leaves(FacetTree tree, Set<StaticNode> leaves)
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
								&& (!tree.isEndPoint(father))) {

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

	// private FacetTree pruneRangesAtRoot(FacetTree tree)
	// throws DatabaseException {
	//
	// // get leaves
	// Set<Node> leaves = tree.getNodesByType(NodeType.LEAVE);
	// Iterator<Node> iter = leaves.iterator();
	//
	// while (iter.hasNext()) {
	//
	// StaticNode currentNode = (StaticNode) iter.next();
	// boolean reachedRangeRoot = false;
	//
	// // walk to root
	//
	// while (!reachedRangeRoot) {
	//
	// Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
	// currentNode).iterator();
	//
	// if (currentNode.isRangeRoot()) {
	//
	// reachedRangeRoot = true;
	//
	// } else if (incomingEdgesIter.hasNext()) {
	//
	// Edge edge2father = incomingEdgesIter.next();
	// Node father = tree.getEdgeSource(edge2father);
	//
	// if (father.isRangeRoot()) {
	//
	// reachedRangeRoot = true;
	//
	// } else {
	//
	// if ((tree.outgoingEdgesOf(father).size() == 1)
	// && (!tree.isEndPoint(father))) {
	//
	// Edge edge2fathersfather = tree.incomingEdgesOf(
	// father).iterator().next();
	//
	// StaticNode fathersfather = (StaticNode) tree
	// .getEdgeSource(edge2fathersfather);
	// fathersfather
	// .setHeight(currentNode.getHeight() + 1);
	//
	// Edge newEdge = tree.addEdge(fathersfather,
	// currentNode);
	// newEdge.setType(EdgeType.SUBCLASS_OF);
	//
	// tree.removeEdge(father, currentNode);
	// tree.removeEdge(fathersfather, father);
	//
	// tree.removeVertex(father);
	//
	// } else {
	//
	// currentNode = (StaticNode) father;
	// }
	// }
	// } else {
	// s_log.error("tree structure is not correct: " + tree);
	// break;
	// }
	// }
	// }
	//
	// return tree;
	// }

	private void updateNodes(FacetTree newTree, String resItem,
			String sourceExt, StaticNode leave) throws DatabaseException,
			IOException, StorageException {

		Queue<Edge> path2RangeRoot = m_cache.getAncestorPath2RangeRoot(newTree,
				leave.getID());

		HashSet<String> objects = m_facetIndex.getObjects(leave, resItem);
		HashSet<String> rangeExt = m_facetIndex.getExtensions(leave, resItem);

		if (objects.size() == 0) {
			s_log.error("no objects found for leave '" + leave + "' and ind '"
					+ resItem + "'!");
		}

		if (!path2RangeRoot.isEmpty()) {

			if (leave.getFacet().getType() != FacetType.DATAPROPERTY_BASED) {

				StaticNode last_node = null;
				StaticNode current_node = null;
				int height = 0;

				while (!path2RangeRoot.isEmpty()) {

					Edge edge = path2RangeRoot.poll();

					current_node = (StaticNode) newTree.getEdgeTarget(edge);
					current_node.addSourceIndivdiual(resItem);
					current_node.addUnsortedObjects(objects);
					current_node.addSourceExtension(sourceExt);
					current_node.addRangeExtensions(rangeExt);
					current_node.incrementCountS(1);

					if (current_node.getHeight() < height) {
						current_node.setHeight(height);
					}

					last_node = (StaticNode) newTree.getEdgeSource(edge);
					height++;
				}

				last_node.addSourceIndivdiual(resItem);
				last_node.addUnsortedObjects(objects);
				last_node.addSourceExtension(sourceExt);
				last_node.addRangeExtensions(rangeExt);
				last_node.incrementCountS(1);

				if (last_node.getHeight() < height) {
					last_node.setHeight(height);
				}
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
				leave.addUnsortedObjects(objects);
			}

			leave.addSourceExtension(sourceExt);
			leave.addRangeExtensions(rangeExt);
			leave.incrementCountS(1);
			leave.setHeight(0);
		}
	}
}
