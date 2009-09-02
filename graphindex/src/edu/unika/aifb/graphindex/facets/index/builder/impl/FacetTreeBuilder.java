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
package edu.unika.aifb.graphindex.facets.index.builder.impl;

import java.io.IOException;
import java.util.ArrayList;
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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.facets.FacetDbUtils;
import edu.unika.aifb.graphindex.facets.FacetEnvironment;
import edu.unika.aifb.graphindex.facets.FacetEnvironment.RDF;
import edu.unika.aifb.graphindex.facets.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.graphindex.facets.model.impl.Edge;
import edu.unika.aifb.graphindex.facets.model.impl.FacetTree;
import edu.unika.aifb.graphindex.facets.model.impl.Node;
import edu.unika.aifb.graphindex.facets.model.impl.Edge.EdgeType;
import edu.unika.aifb.graphindex.facets.model.impl.Node.NodeContent;
import edu.unika.aifb.graphindex.facets.model.impl.Node.NodeType;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
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
public class FacetTreeBuilder implements IFacetIndexBuilder{

	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;

	private Database m_vectorDB;
	private Database m_treeDB;
	private Database m_leaveDB;
	private Database m_endPointDB;
	private Database m_cacheDB;

	private FacetIndexBuilderHelper m_facetHelper;

	private Environment m_env;

	private final static Logger s_log = Logger
			.getLogger(FacetTreeBuilder.class);

	public FacetTreeBuilder(IndexDirectory idxDirectory,
			IndexConfiguration idxConfig, IndexReader idxReader,
			FacetIndexBuilderHelper helper)
			throws EnvironmentLockedException, DatabaseException, IOException {

		this.m_idxDirectory = idxDirectory;
		this.m_idxReader = idxReader;

		this.initDBs();
		this.m_facetHelper = helper;
	}

	public void close() throws DatabaseException {

		this.m_treeDB.close();
		this.m_leaveDB.close();
		this.m_endPointDB.close();
		this.m_cacheDB.close();
		this.m_vectorDB.close();

		this.m_env.removeDatabase(null, FacetDbUtils.DatabaseName.FTB_CACHE);
		this.m_env.close();
	}

	public void build() throws IOException, StorageException,
			DatabaseException {

		IndexStorage spIdx = this.m_idxReader.getStructureIndex()
				.getSPIndexStorage();

		Set<NodeElement> extensions = this.m_facetHelper.getIndexGraph()
				.vertexSet();

		int count = 0;

		for (NodeElement extension : extensions) {
			
			s_log.debug("start building facet tree for extension: " + extension
					+ " (" + (++count) + "/" + extensions.size() + ")");

			FacetTree facetTree = new FacetTree();
			HashMap<Node, ArrayList<String>> endPoints = new HashMap<Node, ArrayList<String>>();

			Set<EdgeElement> properties = this.m_facetHelper.getIndexGraph()
					.outgoingEdgesOf(extension);

			// get property-paths

//			s_log.debug("extension " + extension + " has properties: "
//					+ properties);

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

				facetTree = this.prunePropertyHierarchy(facetTree);

				s_log.debug("inserted properties in facetTree!");
				s_log.debug("start going over endpoints... ");

				List<String> individuals = spIdx.getDataList(
						IndexDescription.EXTENT, DataField.ENT, extension
								.getLabel());

				int extensionSize = individuals.size();

				s_log.debug("extension contains " + extensionSize
						+ " individuals.");

				for (Entry<Node, ArrayList<String>> endpointEntry : endPoints
						.entrySet()) {

					Node property = endpointEntry.getKey();

					for (String individual : individuals) {

						try {

							Table<String> triples = this.m_idxReader
									.getDataIndex().getTriples(individual,
											property.getValue(), null);

							Iterator<String[]> tripleIter = triples.getRows()
									.iterator();

							while (tripleIter.hasNext()) {

								String[] triple = tripleIter.next();
								String object = triple[2];

								if (property.getContent() != NodeContent.DATA_PROPERTY) {

									if (property.getContent() == NodeContent.TYPE_PROPERTY) {

										Stack<Node> classPath = new Stack<Node>();

										Node nodeEndpoint = new Node(object,
												NodeType.ENDPOINT,
												NodeContent.CLASS);
										classPath.push(nodeEndpoint);

										ArrayList<String> keyElements = new ArrayList<String>();
										keyElements.add(extension.getLabel());
										keyElements.add(property.getValue());
										keyElements.add(object);

										String currentClass = object;
										Node superClass;

										while ((superClass = this.m_facetHelper
												.getSuperClass(currentClass)) != null) {

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

										if ((cachedNodes = this
												.getCachedNodes(key)) == null) {

											cachedNodes = this.insertClassPath(
													classPath, facetTree,
													property);

											FacetDbUtils.store(this.m_cacheDB,
													key, cachedNodes);
										}

										this.updateLeaveDB(cachedNodes
												.get(FacetEnvironment.LEAVE),
												extension.getLabel(), object);

										this.updateVectorDB(cachedNodes
												.get(FacetEnvironment.SOURCE),
												extension.getLabel(),
												extensionSize, individual);

									} else if (property.getContent() == NodeContent.OBJECT_PROPERTY) {

										String classLabel = this.m_facetHelper
												.getClass(object);

										if (classLabel != null) {

											Stack<Node> classPath = new Stack<Node>();

											Node nodeEndpoint = new Node(
													classLabel,
													NodeType.ENDPOINT,
													NodeContent.CLASS);
											classPath.push(nodeEndpoint);

											ArrayList<String> keyElements = new ArrayList<String>();
											keyElements.add(extension
													.getLabel());
											keyElements
													.add(property.getValue());
											keyElements.add(classLabel);

											String currentClass = classLabel;
											Node superClass;

											while ((superClass = this.m_facetHelper
													.getSuperClass(currentClass)) != null) {

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

											if ((cachedNodes = this
													.getCachedNodes(key)) == null) {

												// insert path
												cachedNodes = this
														.insertClassPath(
																classPath,
																facetTree,
																property);

												FacetDbUtils.store(
														this.m_cacheDB, key,
														cachedNodes);
											}

											this
													.updateLeaveDB(
															cachedNodes
																	.get(FacetEnvironment.LEAVE),
															extension
																	.getLabel(),
															object);

											this
													.updateVectorDB(
															cachedNodes
																	.get(FacetEnvironment.SOURCE),
															extension
																	.getLabel(),
															extensionSize,
															individual);

										} else {

											ArrayList<String> keyElements = new ArrayList<String>();
											keyElements.add(extension
													.getLabel());
											keyElements
													.add(property.getValue());
											keyElements.add(object);

											String key = FacetDbUtils
													.getKey(keyElements
															.toArray(new String[keyElements
																	.size()]));

											Map<String, Stack<Node>> cachedNodes = null;

											if ((cachedNodes = this
													.getCachedNodes(key)) == null) {

												// insert object
												cachedNodes = this
														.insertObject(
																facetTree,
																property);

												FacetDbUtils.store(
														this.m_cacheDB, key,
														cachedNodes);
											}

											this
													.updateLeaveDB(
															cachedNodes
																	.get(FacetEnvironment.LEAVE),
															extension
																	.getLabel(),
															object);

											this
													.updateVectorDB(
															cachedNodes
																	.get(FacetEnvironment.SOURCE),
															extension
																	.getLabel(),
															extensionSize,
															individual);

										}
									}
								}
								// DataPropery
								else {

									ArrayList<String> keyElements = new ArrayList<String>();
									keyElements.add(extension.getLabel());
									keyElements.add(property.getValue());
									keyElements.add(object);

									String key = FacetDbUtils
											.getKey(keyElements
													.toArray(new String[keyElements
															.size()]));

									Map<String, Stack<Node>> cachedNodes = null;

									if ((cachedNodes = this.getCachedNodes(key)) == null) {

										// insert object
										cachedNodes = this.insertObject(
												facetTree, property);

										FacetDbUtils.store(this.m_cacheDB, key,
												cachedNodes);
									}

									this.updateLeaveDB(cachedNodes
											.get(FacetEnvironment.LEAVE),
											extension.getLabel(), object);

									this.updateVectorDB(cachedNodes
											.get(FacetEnvironment.SOURCE),
											extension.getLabel(),
											extensionSize, individual);
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

				facetTree = this.pruneRanges(facetTree);
			}

			s_log
					.debug("finished facet tree for extension: " + extension
							+ "!");

			FacetDbUtils.store(this.m_endPointDB, extension.getLabel(),
					endPoints);
			FacetDbUtils.store(this.m_treeDB, extension.getLabel(), facetTree);

			System.gc();
		}

		// this.test(this.m_treeDB);
		// this.test(this.m_cacheDB);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Stack<Node>> getCachedNodes(String key)
			throws DatabaseException, IOException {

		Map<String, Stack<Node>> nodes = null;

		Cursor cursor = this.m_cacheDB.openCursor(null, null);

		DatabaseEntry keyEntry = new DatabaseEntry(Util.objectToBytes(key));

		DatabaseEntry out = new DatabaseEntry();

		if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

			if (out.getData() != null) {

				Object object = Util.bytesToObject(out.getData());

				if (object instanceof Map) {
					nodes = (Map<String, Stack<Node>>) object;
				}
			}

		}

		return nodes;
	}

	private void initDBs() throws EnvironmentLockedException,
			DatabaseException, IOException {

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		this.m_env = new Environment(this.m_idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true), envConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);

		this.m_treeDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.TREE, config);

		this.m_leaveDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.LEAVE, config);

		this.m_endPointDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.ENDPOINT, config);

		this.m_vectorDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.VECTOR, config);

		this.m_cacheDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.FTB_CACHE, config);
	}

	private Map<String, Stack<Node>> insertClassPath(Stack<Node> classPath,
			FacetTree facetTree, Node endpoint) throws DatabaseException {

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

				Node rangeTop = this.m_facetHelper.hasRangeClass(currentNode) ? this.m_facetHelper
						.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(endpoint.getValue()),
								NodeType.RANGE_TOP, NodeContent.CLASS);

				facetTree.addVertex(rangeTop);

				Edge edge = facetTree.addEdge(currentNode, rangeTop);
				edge.setType(EdgeType.HAS_RANGE);

				nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);

				if (this.m_facetHelper.isSubClassOf(rangeTop, classPath.peek())) {
					classPath = this.pruneClassPath(rangeTop, classPath);
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

				if (father.getType() == NodeType.ROOT) {
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
			Node endpoint) throws DatabaseException {

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

				Node rangeTop = this.m_facetHelper.hasRangeClass(currentNode) ? this.m_facetHelper
						.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(endpoint.getValue()),
								NodeType.RANGE_TOP, NodeContent.CLASS);

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

				if (father.getType() == NodeType.ROOT) {
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
			HashMap<Node, ArrayList<String>> endPoints) {

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
				endPoints.put(topNode, new ArrayList<String>(topNode
						.getRangeExtensions()));
			} else {

				while (!path.isEmpty()) {

					Node tar = path.pop();
					facetTree.addVertex(tar);

					edge = facetTree.addEdge(topNode, tar);
					edge.setType(EdgeType.SUBPROPERTY_OF);

					if (path.isEmpty()) {
						endPoints.put(tar, new ArrayList<String>(tar
								.getRangeExtensions()));
					}

					topNode = tar;
				}
			}
		}
	}

	private Stack<Node> pruneClassPath(Node rangeTop, Stack<Node> classPath)
			throws DatabaseException {

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

	private FacetTree prunePropertyHierarchy(FacetTree currentTree) {

		// get leaves
		Set<Node> leaves = currentTree.getLeaves();
		Iterator<Node> iter = leaves.iterator();

		while (iter.hasNext()) {

			Node currentNode = iter.next();
			boolean reachedRoot = false;

			// walk to root

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = currentTree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.getType() == NodeType.ROOT) {

					reachedRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = currentTree.getEdgeSource(edge2father);

					if (father.getType() == NodeType.ROOT) {

						reachedRoot = true;

					} else {

						if ((currentTree.outDegreeOf(father) == 1)
								&& (father.getType() != NodeType.ENDPOINT)) {

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

	private FacetTree pruneRanges(FacetTree currentTree)
			throws DatabaseException {

		// get leaves
		Set<Node> leaves = currentTree.getLeaves();
		Iterator<Node> iter = leaves.iterator();

		while (iter.hasNext()) {

			Node currentNode = iter.next();
			boolean reachedRangeRoot = false;

			// walk to root

			while (!reachedRangeRoot) {

				Iterator<Edge> incomingEdgesIter = currentTree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.getType() == NodeType.RANGE_TOP) {

					reachedRangeRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = currentTree.getEdgeSource(edge2father);

					if (father.getType() == NodeType.RANGE_TOP) {

						reachedRangeRoot = true;

					} else {

						if ((currentTree.outgoingEdgesOf(father).size() == 1)
								&& (father.getType() != NodeType.ENDPOINT)) {

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

	@SuppressWarnings("unused")
	private void test(Database db) {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry out = new DatabaseEntry();
		Cursor cursor = null;

		try {
			cursor = db.openCursor(null, null);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		try {
			while (cursor.getNext(key, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				if (out.getData() != null) {

					Object object = Util.bytesToObject(out.getData());

					System.out.println("key = " + key.getData() + " data= "
							+ object);

				}
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private void updateLeaveDB(Stack<Node> leaves, String extension,
			String object) throws DatabaseException, IOException {

		String[] keyElements = new String[] { extension, object };
		String key = FacetDbUtils.getKey(keyElements);

		for (Node leave : leaves) {

			HashSet<Node> nodes = null;

			Cursor cursor = this.m_leaveDB.openCursor(null, null);

			DatabaseEntry keyEntry = new DatabaseEntry(Util.objectToBytes(key));

			DatabaseEntry out = new DatabaseEntry();

			if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				if (out.getData() != null) {

					Object res = Util.bytesToObject(out.getData());

					if (res instanceof HashSet) {
						nodes = (HashSet<Node>) res;
					} else {
						s_log
								.error("found entry for key '"
										+ key
										+ "'. However, it's no HashSet<Node> instance.");
					}
				}
			} else {
				nodes = new HashSet<Node>();
			}

			nodes.add(leave);
			FacetDbUtils.store(this.m_leaveDB, key, nodes);
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
			
			int pos = this.m_facetHelper.getPosition(extension, sub);		
			
			Cursor cursor = this.m_vectorDB.openCursor(null, null);

			DatabaseEntry keyEntry = new DatabaseEntry(Util.objectToBytes(key));

			DatabaseEntry out = new DatabaseEntry();

			if (cursor.getSearchKey(keyEntry, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				if (out.getData() != null) {

					Object res = Util.bytesToObject(out.getData());

					if (res instanceof BitVector) {						
						vector = (BitVector) res;												
					} else {
						s_log.error("found entry for key '" + key
								+ "'. However, it's no bitvector instance.");
					}
				}
			} else {
				vector = new BitVector(extensionSize);
			}
			
			vector.put(pos, true);
			FacetDbUtils.store(this.m_vectorDB, key, vector);
		}
	}
}
