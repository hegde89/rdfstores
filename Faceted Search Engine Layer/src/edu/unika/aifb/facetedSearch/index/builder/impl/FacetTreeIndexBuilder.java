/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package edu.unika.aifb.facetedSearch.index.builder.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import cern.colt.bitvector.BitVector;
import cern.colt.map.HashFunctions;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.RDF;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.api.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge.EdgeType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.index.model.impl.LiteralComparator;
import edu.unika.aifb.facetedSearch.index.model.impl.LiteralList;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetUtil;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.DbConfigFactory;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.EnvironmentFactory;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class FacetTreeIndexBuilder implements IFacetIndexBuilder {

	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;

	private Database m_vectorDB;
	private Database m_treeDB;
	private Database m_leaveDB;
	// private Database m_propertyEndPointDB;
	private Database m_cacheDB;
	private Database m_literalDB;

	private FacetIndexHelper m_facetHelper;

	private Environment m_env;

	private final static Logger s_log = Logger
			.getLogger(FacetTreeIndexBuilder.class);

	public FacetTreeIndexBuilder(IndexDirectory idxDirectory,
			IndexReader idxReader, FacetIndexHelper helper)
			throws EnvironmentLockedException, DatabaseException, IOException {

		this.m_idxDirectory = idxDirectory;
		this.m_idxReader = idxReader;

		this.initDBs();
		this.m_facetHelper = helper;
	}

	public void build() throws IOException, StorageException, DatabaseException {

		IndexStorage spIdx = this.m_idxReader.getStructureIndex()
				.getSPIndexStorage();

		Set<NodeElement> source_extensions = this.m_facetHelper.getIndexGraph()
				.vertexSet();

		int count = 0;

		for (NodeElement source_extension : source_extensions) {

			s_log.debug("start building facet tree for extension: "
					+ source_extension + " (" + (++count) + "/"
					+ source_extensions.size() + ")");

			FacetTree facetTree = new FacetTree();
			HashMap<Node, HashSet<String>> endPoints = new HashMap<Node, HashSet<String>>();

			Set<EdgeElement> properties = this.m_facetHelper.getIndexGraph()
					.outgoingEdgesOf(source_extension);

			// get property-paths
			for (EdgeElement property : properties) {

				String propertyLabel = property.getLabel();

				if (!FacetEnvironment.PROPERTIES_TO_IGNORE
						.contains(propertyLabel)) {

					Stack<Node> propertyPath = new Stack<Node>();
					NodeElement target = property.getTarget();

					if (propertyLabel.equals(RDF.NAMESPACE + RDF.TYPE)) {

						Node endpoint = new Node(propertyLabel,
								NodeType.ENDPOINT, NodeContent.TYPE_PROPERTY);

						endpoint.addRangeExtension(target.getLabel());
						endpoint.setFacet(propertyLabel);
						propertyPath.push(endpoint);

					}
					// data- or object-property
					else {

						Node endpoint = new Node(
								propertyLabel,
								NodeType.ENDPOINT,
								this.m_facetHelper.isDataProperty(property
										.getLabel()) ? NodeContent.DATA_PROPERTY
										: NodeContent.OBJECT_PROPERTY);

						endpoint.addRangeExtension(target.getLabel());
						endpoint.setFacet(propertyLabel);
						propertyPath.push(endpoint);

						String currentProperty = property.getLabel();

						Node superProperty;

						while ((superProperty = this.m_facetHelper
								.getSuperProperty(currentProperty)) != null) {

							propertyPath.push(superProperty);
							currentProperty = superProperty.getValue();
						}
					}

					// insert path
					this.insertPropertyPath(propertyPath, facetTree, endPoints);

				} else {
					s_log.debug("skip property: " + propertyLabel + "!");
				}
			}

			if (!facetTree.isEmpty()) {

				// facetTree = this.prunePropertyHierarchy(facetTree);

				s_log.debug("inserted properties in facetTree!");
				s_log.debug("start going over endpoints... ");

				List<String> individuals = spIdx.getDataList(
						IndexDescription.EXTENT, DataField.ENT,
						source_extension.getLabel());

				int extensionSize = individuals.size();

				s_log.debug("extension contains " + extensionSize
						+ " individuals.");

				for (Entry<Node, HashSet<String>> endpointEntry : endPoints
						.entrySet()) {

					Node property = endpointEntry.getKey();

					for (String individual : individuals) {

						try {

							Table<String> triples = m_idxReader.getDataIndex()
									.getTriples(individual,
											property.getValue(), null);

							Iterator<String[]> tripleIter = triples.getRows()
									.iterator();

							while (tripleIter.hasNext()) {

								String object = tripleIter.next()[2];

								if (property.getContent() != NodeContent.DATA_PROPERTY) {

									if (property.getContent() == NodeContent.TYPE_PROPERTY) {

										Stack<Node> classPath = new Stack<Node>();

										Node nodeEndpoint = new Node(object,
												NodeType.ENDPOINT,
												NodeContent.CLASS);
										nodeEndpoint.setFacet(property
												.getValue());

										classPath.push(nodeEndpoint);

										ArrayList<String> keyElements = new ArrayList<String>();
										keyElements.add(source_extension
												.getLabel());
										keyElements.add(property.getValue());
										keyElements.add(object);

										String currentClass = object;
										Node superClass;

										while ((superClass = m_facetHelper
												.getSuperClass(currentClass)) != null) {

											superClass.setFacet(property
													.getValue());
											classPath.push(superClass);
											currentClass = superClass
													.getValue();

											keyElements.add(superClass
													.getValue());
										}

										String key = FacetDbUtils
												.getKey(keyElements
														.toArray(new String[keyElements
																.size()]));

										Map<String, Stack<Node>> cachedNodes = null;

										if ((cachedNodes = getCachedNodes(key)) == null) {

											cachedNodes = insertClassPath(
													classPath, facetTree,
													property);

											FacetDbUtils.store(m_cacheDB, key,
													cachedNodes);
										}

										updateLeaveDB(cachedNodes
												.get(FacetEnvironment.LEAVE),
												source_extension.getLabel(),
												individual);

										updateVectorDB(cachedNodes
												.get(FacetEnvironment.SOURCE),
												source_extension.getLabel(),
												extensionSize, individual);

									} else if (property.getContent() == NodeContent.OBJECT_PROPERTY) {

										String classLabel = m_facetHelper
												.getClass(object);

										if (classLabel != null) {

											Stack<Node> classPath = new Stack<Node>();

											Node nodeEndpoint = new Node(
													classLabel,
													NodeType.ENDPOINT,
													NodeContent.CLASS);
											nodeEndpoint.setFacet(property
													.getValue());

											classPath.push(nodeEndpoint);

											ArrayList<String> keyElements = new ArrayList<String>();
											keyElements.add(source_extension
													.getLabel());
											keyElements
													.add(property.getValue());
											keyElements.add(classLabel);

											String currentClass = classLabel;
											Node superClass;

											while ((superClass = m_facetHelper
													.getSuperClass(currentClass)) != null) {

												superClass.setFacet(property
														.getValue());
												classPath.push(superClass);
												currentClass = superClass
														.getValue();

												keyElements.add(superClass
														.getValue());
											}

											String key = FacetDbUtils
													.getKey(keyElements
															.toArray(new String[keyElements
																	.size()]));

											Map<String, Stack<Node>> cachedNodes = null;

											if ((cachedNodes = getCachedNodes(key)) == null) {

												// insert path
												cachedNodes = insertClassPath(
														classPath, facetTree,
														property);

												FacetDbUtils.store(m_cacheDB,
														key, cachedNodes);
											}

											updateLeaveDB(
													cachedNodes
															.get(FacetEnvironment.LEAVE),
													source_extension.getLabel(),
													individual);

											updateVectorDB(
													cachedNodes
															.get(FacetEnvironment.SOURCE),
													source_extension.getLabel(),
													extensionSize, individual);

										} else {

											ArrayList<String> keyElements = new ArrayList<String>();
											keyElements.add(source_extension
													.getLabel());
											keyElements
													.add(property.getValue());
											keyElements.add(object);

											String key = FacetDbUtils
													.getKey(keyElements
															.toArray(new String[keyElements
																	.size()]));

											Map<String, Stack<Node>> cachedNodes = null;

											if ((cachedNodes = getCachedNodes(key)) == null) {

												// insert object
												cachedNodes = insertObject(
														facetTree, property);

												FacetDbUtils.store(m_cacheDB,
														key, cachedNodes);
											}

											updateLeaveDB(
													cachedNodes
															.get(FacetEnvironment.LEAVE),
													source_extension.getLabel(),
													individual);

											updateVectorDB(
													cachedNodes
															.get(FacetEnvironment.SOURCE),
													source_extension.getLabel(),
													extensionSize, individual);

										}
									}
								}
								// DataPropery
								else {

									String cacheKey = FacetDbUtils
											.getKey(new String[] {
													source_extension.getLabel(),
													property.getValue(), object });

									Map<String, Stack<Node>> cachedNodes = null;

									if ((cachedNodes = getCachedNodes(cacheKey)) == null) {

										// insert object
										cachedNodes = insertObject(facetTree,
												property);

										FacetDbUtils.store(m_cacheDB, cacheKey,
												cachedNodes);
									}

									updateLeaveDB(cachedNodes
											.get(FacetEnvironment.LEAVE),
											source_extension.getLabel(),
											individual);

									updateVectorDB(cachedNodes
											.get(FacetEnvironment.SOURCE),
											source_extension.getLabel(),
											extensionSize, individual);

									updateLiteralDB(
											source_extension.getLabel(),
											property.getValue(), individual);
								}
							}
						} catch (StorageException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (NullPointerException e) {
							e.printStackTrace();
						}
					}
				}

				// facetTree = this.pruneRanges(facetTree);
			}

			s_log.debug("finished facet tree for extension: "
					+ source_extension + "!");

			// // store endpoints
			// FacetDbUtils.store(this.m_propertyEndPointDB, source_extension
			// .getLabel(), endPoints);

			// set hashvalues for each node contained in current tree
			setPathAndPathHashValue(facetTree);

			// sort the literal lists for the dataproperties
			sortLiteralLists(source_extension.getLabel(), endPoints);

			// store tree
			FacetDbUtils.store(this.m_treeDB, source_extension.getLabel(),
					facetTree);

			System.gc();
		}
	}

	public void close() throws DatabaseException {

		if (m_treeDB != null) {
			m_treeDB.close();
		}

		if (m_leaveDB != null) {
			m_leaveDB.close();
		}

		// if (m_propertyEndPointDB != null) {
		// m_propertyEndPointDB.close();
		// }

		if (m_cacheDB != null) {
			m_cacheDB.close();
		}

		if (m_vectorDB != null) {
			m_vectorDB.close();
		}

		if (m_literalDB != null) {
			m_literalDB.close();
		}

		if (m_env != null) {
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FTB_CACHE);
			m_env.close();
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Stack<Node>> getCachedNodes(String key)
			throws DatabaseException, IOException {

		return (Map<String, Stack<Node>>) FacetDbUtils.get(m_cacheDB, key);
	}

	private void initDBs() throws EnvironmentLockedException,
			DatabaseException, IOException {

		this.m_env = EnvironmentFactory.make(this.m_idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true));

		// Databases without duplicates
		DatabaseConfig config = DbConfigFactory.make(false);

		this.m_treeDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.TREE, config);

		// this.m_propertyEndPointDB = this.m_env.openDatabase(null,
		// FacetDbUtils.DatabaseNames.ENDPOINT, config);

		this.m_vectorDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.VECTOR, config);

		this.m_cacheDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FTB_CACHE, config);

		this.m_literalDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.LITERAL, config);

		this.m_leaveDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.LEAVE, config);

		// // Databases with duplicates
		// DatabaseConfig config2 = DbConfigFactory.make(true);
		//
		// this.m_leaveDB = this.m_env.openDatabase(null,
		// FacetDbUtils.DatabaseNames.LEAVE, config2);

	}

	private Map<String, Stack<Node>> insertClassPath(Stack<Node> classPath,
			FacetTree facetTree, Node endpoint) throws DatabaseException,
			IOException {

		// init
		Map<String, Stack<Node>> nodes2cache = new HashMap<String, Stack<Node>>();
		nodes2cache.put(FacetEnvironment.SOURCE, new Stack<Node>());
		nodes2cache.put(FacetEnvironment.LEAVE, new Stack<Node>());

		Node currentNode = endpoint;
		boolean reachedRoot = false;

		do {

			boolean foundRange = false;
			Iterator<Edge> iter_outEdges = facetTree.outgoingEdgesOf(
					currentNode).iterator();

			while (iter_outEdges.hasNext()) {

				Edge thisEdge = iter_outEdges.next();

				if (thisEdge.getType() == EdgeType.HAS_RANGE) {

					foundRange = true;

					Node rangeTop = facetTree.getEdgeTarget(thisEdge);

					nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);

					Stack<Edge> rangeEdges = new Stack<Edge>();
					rangeEdges.addAll(facetTree.outgoingEdgesOf(rangeTop));

					if (this.m_facetHelper.isSubClassOf(rangeTop, classPath
							.peek())) {
						classPath = this.pruneClassPath(rangeTop, classPath);
					}

					if (rangeTop.equals(classPath.peek())) {
						classPath.pop();
					}

					if (classPath.isEmpty()) {

						nodes2cache.get(FacetEnvironment.LEAVE).add(rangeTop);

					} else {

						Node topNode = classPath.peek();

						while (!rangeEdges.isEmpty() && !classPath.isEmpty()) {

							Node child = facetTree.getEdgeTarget(rangeEdges
									.pop());

							if (child.equals(topNode)) {

								nodes2cache.get(FacetEnvironment.SOURCE).add(
										child);
								classPath.pop();

								if (classPath.isEmpty()) {

									nodes2cache.get(FacetEnvironment.LEAVE)
											.add(child);

								} else {

									topNode = classPath.peek();
									rangeTop = child;
									rangeEdges.clear();
									rangeEdges.addAll(facetTree
											.outgoingEdgesOf(child));
								}
							}
						}

						if (rangeEdges.isEmpty() && !classPath.isEmpty()) {

							topNode = classPath.pop();
							facetTree.addVertex(topNode);

							nodes2cache.get(FacetEnvironment.SOURCE).add(
									topNode);

							Edge edge = facetTree.addEdge(rangeTop, topNode);
							edge.setType(EdgeType.SUBCLASS_OF);

							if (classPath.isEmpty()) {

								nodes2cache.get(FacetEnvironment.LEAVE).add(
										topNode);

							} else {

								while (!classPath.isEmpty()) {

									Node tar = classPath.pop();
									facetTree.addVertex(tar);

									edge = facetTree.addEdge(topNode, tar);
									edge.setType(EdgeType.SUBPROPERTY_OF);

									nodes2cache.get(FacetEnvironment.SOURCE)
											.add(tar);

									if (classPath.isEmpty()) {

										nodes2cache.get(FacetEnvironment.LEAVE)
												.add(tar);
									}

									topNode = tar;
								}
							}
						}
					}
				}
			}
			if (!foundRange) {

				// TODO: code okay?

				Node rangeTop = m_facetHelper.hasRangeClass(currentNode) ? m_facetHelper
						.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(endpoint.getValue()),
								NodeType.RANGE_ROOT, NodeContent.CLASS);

				rangeTop.setFacet(endpoint.getValue());

				facetTree.addVertex(rangeTop);

				Edge edge = facetTree.addEdge(currentNode, rangeTop);
				edge.setType(EdgeType.HAS_RANGE);

				nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);

				if (m_facetHelper.isSubClassOf(rangeTop, classPath.peek())) {
					classPath = pruneClassPath(rangeTop, classPath);
				}

				if (rangeTop.equals(classPath.peek())) {
					classPath.pop();
				}

				if (!classPath.isEmpty()) {

					Node classPathTop = classPath.pop();
					facetTree.addVertex(classPathTop);

					edge = facetTree.addEdge(rangeTop, classPathTop);
					edge.setType(EdgeType.SUBCLASS_OF);

					nodes2cache.get(FacetEnvironment.SOURCE).add(classPathTop);

					if (classPath.isEmpty()) {

						nodes2cache.get(FacetEnvironment.LEAVE).add(
								classPathTop);

					} else {

						while (!classPath.isEmpty()) {

							Node tar = classPath.pop();
							facetTree.addVertex(tar);

							edge = facetTree.addEdge(classPathTop, tar);
							edge.setType(EdgeType.SUBPROPERTY_OF);

							nodes2cache.get(FacetEnvironment.SOURCE).add(tar);

							if (classPath.isEmpty()) {

								nodes2cache.get(FacetEnvironment.LEAVE)
										.add(tar);
							}

							classPathTop = tar;
						}
					}
				} else {

					nodes2cache.get(FacetEnvironment.LEAVE).add(rangeTop);
				}
			}

			Iterator<Edge> incomingEdgesIter = facetTree.incomingEdgesOf(
					currentNode).iterator();

			if (incomingEdgesIter.hasNext()) {

				Edge edge2father = incomingEdgesIter.next();
				Node father = facetTree.getEdgeSource(edge2father);

				if (father.isRoot()) {
					reachedRoot = true;
				} else {
					currentNode = father;
				}
			} else {
				s_log.error("Tree structure is not correct: " + facetTree);
				break;
			}
		} while (!reachedRoot);

		return nodes2cache;
	}

	private Map<String, Stack<Node>> insertObject(FacetTree facetTree,
			Node endpoint) throws DatabaseException, IOException {

		// init
		Map<String, Stack<Node>> nodes2cache = new HashMap<String, Stack<Node>>();
		nodes2cache.put(FacetEnvironment.SOURCE, new Stack<Node>());
		nodes2cache.put(FacetEnvironment.LEAVE, new Stack<Node>());

		Node currentNode = endpoint;
		boolean reachedRoot = false;

		do {

			boolean foundRange = false;
			Iterator<Edge> iter_outEdges = facetTree.outgoingEdgesOf(
					currentNode).iterator();

			while (iter_outEdges.hasNext()) {

				Edge thisEdge = iter_outEdges.next();

				if (thisEdge.getType() == EdgeType.HAS_RANGE) {

					foundRange = true;

					Node rangeTop = facetTree.getEdgeTarget(thisEdge);

					nodes2cache.get(FacetEnvironment.LEAVE).add(rangeTop);
					nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);
				}
			}

			if (!foundRange) {

				// TODO: code okay?

				Node rangeTop = this.m_facetHelper.hasRangeClass(currentNode) ? this.m_facetHelper
						.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(endpoint.getValue()),
								NodeType.RANGE_ROOT, NodeContent.CLASS);

				rangeTop.setFacet(endpoint.getValue());
				facetTree.addVertex(rangeTop);

				Edge edge = facetTree.addEdge(currentNode, rangeTop);
				edge.setType(EdgeType.HAS_RANGE);

				nodes2cache.get(FacetEnvironment.LEAVE).add(rangeTop);
				nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);
			}

			Iterator<Edge> incomingEdgesIter = facetTree.incomingEdgesOf(
					currentNode).iterator();

			if (incomingEdgesIter.hasNext()) {

				Edge edge2father = incomingEdgesIter.next();
				Node father = facetTree.getEdgeSource(edge2father);

				if (father.isRoot()) {
					reachedRoot = true;
				} else {
					currentNode = father;
				}
			} else {
				s_log.error("Tree structure is not correct: " + facetTree);
				break;
			}
		} while (!reachedRoot);

		return nodes2cache;
	}

	private void insertPropertyPath(Stack<Node> path, FacetTree facetTree,
			HashMap<Node, HashSet<String>> endPoints) {

		Node currentNode = facetTree.getRoot();
		Stack<Edge> edgesStack = new Stack<Edge>();
		edgesStack.addAll(facetTree.outgoingEdgesOf(currentNode));

		boolean containsPath = false;
		Node topNode = path.pop();

		while (!edgesStack.isEmpty()) {

			Edge currentEdge = edgesStack.pop();
			Node currentTarget = facetTree.getEdgeTarget(currentEdge);

			if (currentTarget.equals(topNode)) {

				currentNode = currentTarget;

				if (path.isEmpty()) {

					containsPath = true;
					endPoints.get(currentTarget).addAll(
							currentTarget.getRangeExtensions());
				} else {

					edgesStack.clear();
					edgesStack.addAll(facetTree.outgoingEdgesOf(currentTarget));
					topNode = path.pop();
				}
			}
		}
		if (!containsPath) {

			facetTree.addVertex(topNode);
			Edge edge = facetTree.addEdge(currentNode, topNode);
			edge.setType(EdgeType.SUBPROPERTY_OF);

			if (path.isEmpty()) {
				endPoints.put(topNode, new HashSet<String>(topNode
						.getRangeExtensions()));
			} else {

				while (!path.isEmpty()) {

					Node tar = path.pop();
					facetTree.addVertex(tar);

					edge = facetTree.addEdge(topNode, tar);
					edge.setType(EdgeType.SUBPROPERTY_OF);

					if (path.isEmpty()) {
						endPoints.put(tar, new HashSet<String>(tar
								.getRangeExtensions()));
					}

					topNode = tar;
				}
			}
		}
	}

	private Stack<Node> pruneClassPath(Node rangeTop, Stack<Node> classPath)
			throws DatabaseException, IOException {

		while (!classPath.isEmpty()) {

			Node pathTop = classPath.peek();

			if (this.m_facetHelper.isSubClassOf(rangeTop, pathTop)) {
				classPath.pop();
			} else {
				break;
			}
		}

		return classPath;
	}

	@SuppressWarnings("unused")
	private FacetTree prunePropertyHierarchy(FacetTree currentTree) {

		// get leaves
		Set<Node> leaves = currentTree.getNodesByType(NodeType.LEAVE);
		Iterator<Node> iter = leaves.iterator();

		while (iter.hasNext()) {

			Node currentNode = iter.next();
			boolean reachedRoot = false;

			// walk to root

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = currentTree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.isRoot()) {

					reachedRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = currentTree.getEdgeSource(edge2father);

					if (father.isRoot()) {

						reachedRoot = true;

					} else {

						if ((currentTree.outDegreeOf(father) == 1)
								&& (father.isEndPoint())) {

							Edge edge2fathersfather = currentTree
									.incomingEdgesOf(father).iterator().next();

							Node fathersfather = currentTree
									.getEdgeSource(edge2fathersfather);

							Edge newEdge = currentTree.addEdge(fathersfather,
									currentNode);
							newEdge.setType(EdgeType.SUBPROPERTY_OF);

							currentTree.removeEdge(father, currentNode);
							currentTree.removeEdge(fathersfather, father);
							currentTree.removeVertex(father);

							// currentNode = fathersfather;

						} else {

							currentNode = father;
						}
					}
				} else {
					s_log
							.error("Tree structure is not correct: "
									+ currentTree);
					break;
				}
			}
		}

		return currentTree;
	}

	@SuppressWarnings("unused")
	private FacetTree pruneRanges(FacetTree currentTree)
			throws DatabaseException {

		// get leaves
		Set<Node> leaves = currentTree.getNodesByType(NodeType.LEAVE);
		Iterator<Node> iter = leaves.iterator();

		while (iter.hasNext()) {

			Node currentNode = iter.next();
			boolean reachedRangeRoot = false;

			// walk to root

			while (!reachedRangeRoot) {

				Iterator<Edge> incomingEdgesIter = currentTree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.isRangeRoot()) {

					reachedRangeRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = currentTree.getEdgeSource(edge2father);

					if (father.isRangeRoot()) {

						reachedRangeRoot = true;

					} else {

						if ((currentTree.outgoingEdgesOf(father).size() == 1)
								&& (father.isEndPoint())) {

							Edge edge2fathersfather = currentTree
									.incomingEdgesOf(father).iterator().next();

							Node fathersfather = currentTree
									.getEdgeSource(edge2fathersfather);

							Edge newEdge = currentTree.addEdge(fathersfather,
									currentNode);
							newEdge.setType(EdgeType.SUBCLASS_OF);

							currentTree.removeEdge(father, currentNode);
							currentTree.removeEdge(fathersfather, father);

							currentTree.removeVertex(father);

							// currentNode = fathersfather;

						} else {

							currentNode = father;
						}
					}
				} else {
					s_log
							.error("tree structure is not correct: "
									+ currentTree);
					break;
				}
			}
		}

		return currentTree;
	}

	// @SuppressWarnings("unused")
	// private void test(Database db) {
	//
	// DatabaseEntry key = new DatabaseEntry();
	// DatabaseEntry out = new DatabaseEntry();
	// Cursor cursor = null;
	//
	// try {
	// cursor = db.openCursor(null, null);
	// } catch (DatabaseException e) {
	// e.printStackTrace();
	// }
	//
	// try {
	// while (cursor.getNext(key, out, LockMode.DEFAULT) ==
	// OperationStatus.SUCCESS) {
	//
	// if (out.getData() != null) {
	//
	// Object object = Util.bytesToObject(out.getData());
	//
	// System.out.println("key = " + key.getData() + " data= "
	// + object);
	//
	// }
	// }
	// } catch (DatabaseException e) {
	// e.printStackTrace();
	// }
	// }

	private void setPathAndPathHashValue(FacetTree tree) {

		// nodes to update ...
		List<Node> nodes = new ArrayList<Node>();
		nodes.addAll(tree.vertexSet());
		nodes.remove(tree.getRoot());

		for (Node node : nodes) {

			boolean reachedRoot = false;
			Node currentNode = node;
			String path = "";

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
						currentNode).iterator();

				if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = tree.getEdgeSource(edge2father);
					path = father.getValue() + path;

					if (father.isRoot()) {
						reachedRoot = true;
					} else {
						currentNode = father;
					}
				} else {
					s_log.error("tree structure is not correct: " + this);
					break;
				}
			}

			path = currentNode.getValue() + path;

			node.setPathHashValue(HashFunctions.hash(path));
			node.setPath(path);

		}
	}

	private void sortLiteralLists(String extension,
			HashMap<Node, HashSet<String>> endPoints) throws DatabaseException,
			IOException {

		s_log.debug("start sorting literals for extension '" + extension + "'");

		for (Node prop : endPoints.keySet()) {

			if (prop.getContent() == NodeContent.DATA_PROPERTY) {

				String key = FacetDbUtils.getKey(new String[] { extension,
						prop.getValue() });

				LiteralList literalList = (LiteralList) FacetDbUtils.get(
						m_literalDB, key);

				// sort list
				Collections.sort(literalList.getLiterals(),
						new LiteralComparator());

				// System.out.println("Sorted list:");
				// System.out.println("---------------------------");
				//
				// for (ILiteral object : objects) {
				// System.out.println("object: " + object.getValue());
				// System.out.println();
				// }
				//
				// System.out.println("---------------------------");

				// store list

				FacetDbUtils.store(m_literalDB, key, literalList);
			}
		}

		s_log.debug("finished sorting literals for extension: " + extension
				+ "!");

	}

	@SuppressWarnings("unchecked")
	private void updateLeaveDB(Stack<Node> leaves, String extension,
			String individual) throws DatabaseException, IOException {

		// String[] keyElements = new String[] { extension, object };
		// String key = FacetDbUtils.getKey(keyElements);
		//
		// for (Node leave : leaves) {
		// FacetDbUtils.store(m_leaveDB, key, leave);
		// }

		String[] keyElements = new String[] { extension, individual };
		String key = FacetDbUtils.getKey(keyElements);

		HashSet<Node> nodes = null;

		try {

			if ((nodes = (HashSet<Node>) FacetDbUtils.get(m_leaveDB, key)) == null) {
				nodes = new HashSet<Node>();
			}

		} catch (ClassCastException e) {

			s_log.error("found entry for key '" + key
					+ "'. However, it's no HashSet<Node> instance.");
			nodes = new HashSet<Node>();
		}

		for (Node leave : leaves) {
			nodes.add(leave);
		}

		FacetDbUtils.store(m_leaveDB, key, nodes);
	}

	private void updateLiteralDB(String extension, String endpointProp,
			String literalString) throws DatabaseException, IOException,
			StorageException {

		String key = FacetDbUtils
				.getKey(new String[] { extension, endpointProp });

		LiteralList literalList = null;

		try {

			if ((literalList = (LiteralList) FacetDbUtils.get(m_literalDB, key)) == null) {
				literalList = new LiteralList();
			}

		} catch (ClassCastException e) {

			s_log.error("found entry for key '" + key
					+ "'. However, it's no LiteralList instance.");

			literalList = new LiteralList();
		}

		ILiteral lit = new Literal(FacetUtil.getLiteralValue(literalString),
				FacetUtil.getLiteralDataType(literalString));

		if (!literalList.contains(lit)) {
			literalList.add(lit);
			FacetDbUtils.store(this.m_literalDB, key, literalList);
		}
	}

	private void updateVectorDB(Stack<Node> nodes, String extension,
			int extensionSize, String sub) throws DatabaseException,
			IOException, StorageException {

		for (Node node : nodes) {

			String[] keyElements = new String[] { extension,
					String.valueOf(node.getID()) };

			String key = FacetDbUtils.getKey(keyElements);

			BitVector vector = null;
			int pos = m_facetHelper.getPosition(extension, sub);

			try {

				if ((vector = (BitVector) FacetDbUtils.get(m_vectorDB, key)) == null) {
					vector = new BitVector(extensionSize);
				}

			} catch (ClassCastException e) {

				s_log.error("found entry for key '" + key
						+ "'. However, it's no bitvector instance.");

				vector = new BitVector(extensionSize);
			}

			if (!vector.get(pos)) {
				vector.put(pos, true);
				FacetDbUtils.store(m_vectorDB, key, vector);
			}
		}
	}
}
