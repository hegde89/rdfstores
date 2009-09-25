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
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.ClusterDistance;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.DistanceComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.distance.PositionComparator;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.metric.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.algo.construction.tree.IFacetTreeBuilder;
import edu.unika.aifb.facetedSearch.facets.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet.FacetType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge.EdgeType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree.EndPointType;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache.CleanType;
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

	// private ComparatorPool m_compPool;

	private SearchSession m_session;
	private SearchSessionCache m_cache;

	private HashMap<String, FacetTree> m_indexedTrees;
	private HashMap<Integer, StaticNode> m_paths;

	/*
	 * Indices
	 */
	private FacetIndex m_facetIndex;
	private FacetTreeDelegator m_treeDelegator;
	private StructureIndex m_structureIndex;

	public FacetTreeBuilder(SearchSession session) throws IOException,
			EnvironmentLockedException, DatabaseException, StorageException {

		m_session = session;
		m_treeDelegator = (FacetTreeDelegator) session
				.getDelegator(Delegators.TREE);
		m_cache = session.getCache();

		/*
		 * indices
		 */

		m_facetIndex = (FacetIndex) m_session.getStore().getIndex(
				IndexName.FACET_INDEX);
		m_structureIndex = (StructureIndex) m_session.getStore().getIndex(
				IndexName.STRUCTURE_INDEX);

		/*
		 * init
		 */

		m_indexedTrees = new HashMap<String, FacetTree>();
		m_paths = new HashMap<Integer, StaticNode>();

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

	public boolean constructCluster(FacetTree tree, StaticNode node) {

		clear();

		if (node.getFacet().isDataPropertyBased()) {

			if (node instanceof FacetValueNode) {
				return false;
			} else {

				long time1 = System.currentTimeMillis();

				try {

					doClustering(tree, node);

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (DatabaseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				long time2 = System.currentTimeMillis();

				s_log.debug("did clustering for node '" + node + "' in "
						+ (time2 - time1) + " ms!");
				return true;
			}

		} else {
			s_log.error("facet " + node.getFacet() + " has invalid facetType!");
			return false;
		}
	}

	public boolean constructSubTree(StaticNode node) throws StorageException,
			IOException, DatabaseException {

		clear();
		long time1 = System.currentTimeMillis();

		if (node.getFacet().isObjectPropertyBased()) {

			FacetTree subTree = new FacetTree();
			subTree.setDomain(node.getDomain());

			Set<StaticNode> newLeaves = new HashSet<StaticNode>();
			Iterator<FacetValue> indIter = node.getObjects().iterator();

			if (!(node instanceof FacetValueNode) && indIter.hasNext()) {

				while (indIter.hasNext()) {

					FacetValue fv = indIter.next();
					String ext = fv.getExt();

					FacetTree currentIndexedTree;

					if (!m_indexedTrees.containsKey(ext)) {

						FacetTree indexedTree = m_facetIndex.getTree(ext);
						m_indexedTrees.put(ext, indexedTree);

						currentIndexedTree = indexedTree;

					} else {

						currentIndexedTree = m_indexedTrees.get(ext);

					}

					Collection<Double> oldLeaves = m_facetIndex.getLeaves(fv);

					for (double leave : oldLeaves) {

						StaticNode newLeave = insertPathAtRoot(subTree,
								currentIndexedTree, leave);
						newLeaves.add(newLeave);

						updateNodeCounts(subTree, fv.getValue(), ext, newLeave);

					}
				}

				// prune ranges
				subTree = pruneRanges(subTree, newLeaves);

				doClustering(subTree, subTree
						.getEndPoints(EndPointType.DATA_PROPERTY));

				doAttachRDFTypes(subTree, subTree
						.getEndPoints(EndPointType.RDF_PROPERTY));

				long time2 = System.currentTimeMillis();

				s_log.debug("constructed subtree for node '" + node + "' in "
						+ (time2 - time1) + " ms!");

				m_treeDelegator.addSubTree4Node(node, subTree);

				return true;

			} else {
				return false;
			}
		} else {
			s_log.error("facet " + node.getFacet() + " has invalid facetType!");
			return false;
		}
	}

	public boolean constructTree(Table<String> results, int column)
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

			Collection<Double> oldLeaves = m_facetIndex.getLeaves(
					sourceExtension, resItem);

			for (double leave : oldLeaves) {

				StaticNode newLeave = insertPathAtRoot(newTree,
						currentIndexedTree, leave);
				newLeaves.add(newLeave);

				updateNodeCounts(newTree, resItem, sourceExtension, newLeave);

			}
		}

		// prune ranges
		newTree = pruneRanges(newTree, newLeaves);

		doClustering(newTree, newTree.getEndPoints(EndPointType.DATA_PROPERTY));
		doConstructSubtrees(newTree.getEndPoints(EndPointType.OBJECT_PROPERTY));
		doAttachRDFTypes(newTree, newTree
				.getEndPoints(EndPointType.RDF_PROPERTY));

		long time2 = System.currentTimeMillis();

		s_log.debug("constructed tree for domain '"
				+ results.getColumnName(column) + "' in " + (time2 - time1)
				+ " ms!");

		m_treeDelegator.addTree4Domain(results.getColumnName(column), newTree);
		return true;
	}

	private void doAttachRDFTypes(FacetTree tree, Set<StaticNode> rdfprop_eps)
			throws DatabaseException, IOException {

		for (StaticNode node : rdfprop_eps) {
			insertFacetValues(tree, node, node.getObjects());
		}
	}

	private void doClustering(FacetTree tree, Set<StaticNode> dataprop_eps)
			throws DatabaseException, IOException, StorageException {

		Iterator<StaticNode> epIter = dataprop_eps.iterator();

		while (epIter.hasNext()) {

			StaticNode epNode = epIter.next();
			doClustering(tree, epNode);
		}
	}

	@SuppressWarnings("unchecked")
	private void doClustering(FacetTree tree, StaticNode epNode)
			throws DatabaseException, UnsupportedEncodingException, IOException {

		m_cache.clean(CleanType.LITERALS);

		DataType datatype = epNode.getFacet().getDataType() == null
				? FacetEnvironment.DataType.STRING
				: epNode.getFacet().getDataType();

		IDistanceMetric metric = DistanceMetricPool.getMetric(datatype);

		Collection<Literal> lits = epNode.getSortedLiterals();
		ArrayList<Literal> litsList = new ArrayList<Literal>(lits);

		// /*
		// * merge sort: O(nlogn)
		// */
		// Collections.sort(lits, m_compPool.getComparator(datatype));

		if (lits.size() > FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE) {

			// construct and sort distances for node

			/*
			 * each insert has O(logn)
			 */
			PriorityQueue<ClusterDistance> distanceQueue = new PriorityQueue<ClusterDistance>(
					FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1,
					new DistanceComparator());

			Iterator<Literal> litIter = lits.iterator();

			int current_leftCountS = 0;
			int current_leftCountFV = 0;

			ArrayList<ClusterDistance> current_leftDistances = new ArrayList<ClusterDistance>();
			HashSet<String> current_leftSources = new HashSet<String>();

			while (litIter.hasNext()) {

				Literal current_left = litIter.next();
				String ext = current_left.getExt();

				if (litIter.hasNext()) {

					Literal current_right = litIter.next();
					ClusterDistance clusterDistance;

					if ((clusterDistance = m_cache.getDistance(current_left
							.getValue(), current_right.getValue(), ext)) == null) {

						current_leftSources.addAll(m_cache
								.getSources4FacetValue(current_left));
						current_leftCountS = current_leftSources.size();
						current_leftCountFV += 1;

						BigDecimal distanceValue = metric.getDistance(
								current_left.getParsedLiteral(), current_right
										.getParsedLiteral());

						clusterDistance = new ClusterDistance(current_left
								.getValue(), current_right.getValue());

						clusterDistance.setLeftDistances(current_leftDistances);
						clusterDistance.setValue(distanceValue);
						clusterDistance.setLeftCountFV(current_leftCountFV);
						clusterDistance.setLeftCountS(current_leftCountS);

						m_cache.addDistance(current_left.getValue(),
								current_right.getValue(), ext, clusterDistance);

					}

					boolean success = distanceQueue.offer(clusterDistance);

					if (!success) {

						s_log.error("could not insert distance '"
								+ clusterDistance + "'!");

					}

					current_leftDistances.add(clusterDistance);
				}
			}

			/*
			 * O(klogk)
			 */

			PriorityQueue<ClusterDistance> posQueue = new PriorityQueue<ClusterDistance>(
					FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1,
					new PositionComparator());

			for (int i = 0; i < FacetEnvironment.DefaultValue.NUM_OF_CHILDREN_PER_NODE - 1; i++) {

				ClusterDistance clusterDistance = distanceQueue.poll();
				posQueue.offer(clusterDistance);

			}

			Edge edge;
			DynamicNode dynNode;
			ClusterDistance firstDis;
			ClusterDistance secondDis;

			if (!posQueue.isEmpty()) {

				// first distance
				firstDis = posQueue.poll();

				dynNode = new DynamicNode();
				dynNode.setDomain(epNode.getDomain());
				dynNode.setCache(m_cache);
				dynNode.setCountS(firstDis.getLeftCountS());
				dynNode.setCountFV(firstDis.getLeftCountFV());
				dynNode.setLeftBorder(litsList.get(0).getValue());
				dynNode.setRightBorder(firstDis.getLeftBorder());
				dynNode.setLits(litsList.subList(0, litsList.indexOf(firstDis
						.getLeftBorder())));

				tree.addVertex(dynNode);
				edge = tree.addEdge(epNode, dynNode);
				edge.setType(EdgeType.CONTAINS);

				while (!posQueue.isEmpty()) {

					secondDis = posQueue.poll();

					dynNode = new DynamicNode();
					dynNode.setDomain(epNode.getDomain());
					dynNode.setCache(m_cache);
					dynNode.setCountS(secondDis.getLeftCountS()
							- firstDis.getLeftCountS());
					dynNode.setCountFV(secondDis.getLeftCountFV()
							- firstDis.getLeftCountFV());
					dynNode.setLeftBorder(firstDis.getRightBorder());
					dynNode.setRightBorder(secondDis.getLeftBorder());
					dynNode.setLits(litsList.subList(litsList.indexOf(firstDis
							.getRightBorder()), litsList.indexOf(secondDis
							.getLeftBorder())));

					tree.addVertex(dynNode);
					edge = tree.addEdge(epNode, dynNode);
					edge.setType(EdgeType.CONTAINS);

					if (posQueue.isEmpty()) {

						dynNode = new DynamicNode();
						dynNode.setDomain(epNode.getDomain());
						dynNode.setCache(m_cache);
						dynNode.setCountS(epNode.getCountS()
								- secondDis.getLeftCountS());
						dynNode.setCountFV(epNode.getCountFV()
								- secondDis.getLeftCountFV());
						dynNode.setLeftBorder(secondDis.getLeftBorder());
						dynNode.setRightBorder(litsList.get(lits.size() - 1)
								.getValue());
						dynNode.setLits(litsList.subList(litsList
								.indexOf(secondDis.getRightBorder()), litsList
								.size() - 1));

						tree.addVertex(dynNode);
						edge = tree.addEdge(epNode, dynNode);
						edge.setType(EdgeType.CONTAINS);

					} else {
						firstDis = secondDis;
					}
				}
			}

		} else {

			insertFacetValues(tree, epNode, lits);
		}
	}

	private void doConstructSubtrees(Set<StaticNode> objectprop_eps) {

		for (StaticNode node : objectprop_eps) {

			try {

				constructSubTree(node);

			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
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

	private void insertFacetValues(FacetTree tree, StaticNode node,
			Collection<? extends FacetValue> values) {

		Iterator<? extends FacetValue> valueIter = values.iterator();

		while (valueIter.hasNext()) {

			FacetValue fv = valueIter.next();
			FacetValueNode fvNode = new FacetValueNode(fv.getValue());
			fvNode.setDomain(node.getDomain());
			fvNode.setCache(m_cache);
			fvNode.setDepth(node.getDepth() + 1);

		}
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

	private FacetTree pruneRanges(FacetTree tree, Set<StaticNode> leaves)
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

	private void updateNodeCounts(FacetTree newTree, String resItem,
			String sourceExt, StaticNode leave) throws DatabaseException,
			IOException, StorageException {

		Queue<Edge> path2RangeRoot = m_cache.getAncestorPath2RangeRoot(newTree,
				leave.getID());

		Collection<FacetValue> objects = m_facetIndex
				.getObjects(leave, resItem);

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
