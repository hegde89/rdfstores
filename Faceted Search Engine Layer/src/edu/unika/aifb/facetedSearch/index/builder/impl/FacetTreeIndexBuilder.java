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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.RDF;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;
import edu.unika.aifb.facetedSearch.facets.tree.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.index.builder.cache.LRUCache;
import edu.unika.aifb.facetedSearch.index.db.binding.AbstractSingleFacetValueBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.NodeBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.PathBinding;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
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

	/*
	 * 
	 */
	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;

	/*
	 * 
	 */
	private Environment m_env;
	private Environment m_env2;

	/*
	 * 
	 */
	private Database m_pathDB;
	private Database m_leaveDB;
	private Database m_objectDB;

	/*
	 * 
	 */
	private EntryBinding<AbstractSingleFacetValue> m_fvBinding;
	private EntryBinding<Queue<Edge>> m_pathBinding;
	private EntryBinding<Node> m_nodeBinding;
	private EntryBinding<String> m_strgBinding;

	/*
	 * 
	 */
	private StoredMap<String, AbstractSingleFacetValue> m_objectMap;
	private StoredMap<String, Node> m_leaveMap;

	/*
	 * 
	 */
	private FacetIndexHelper m_facetHelper;

	private final static Logger s_log = Logger
			.getLogger(FacetTreeIndexBuilder.class);

	public FacetTreeIndexBuilder(IndexDirectory idxDirectory,
			IndexReader idxReader, FacetIndexHelper helper)
			throws EnvironmentLockedException, DatabaseException, IOException {

		m_facetHelper = helper;
		m_idxDirectory = idxDirectory;
		m_idxReader = idxReader;

		init();
	}

	public void build() throws IOException, StorageException, DatabaseException {

		LRUCache<String, Stack<Node>> cache = new LRUCache<String, Stack<Node>>(
				5000);

		IndexStorage spIdx = m_idxReader.getStructureIndex()
				.getSPIndexStorage();

		Set<NodeElement> source_extensions = m_facetHelper.getIndexGraph()
				.vertexSet();

		int count = 0;

		for (NodeElement source_extension : source_extensions) {

			cache.clear();

			s_log.debug("start building facet tree for extension: "
					+ source_extension + " (" + (++count) + "/"
					+ source_extensions.size() + ")");

			FacetTree facetTree = new FacetTree();
			HashMap<Node, HashSet<String>> endPoints = new HashMap<Node, HashSet<String>>();

			Set<EdgeElement> properties = m_facetHelper.getIndexGraph()
					.outgoingEdgesOf(source_extension);

			// get property-paths
			for (EdgeElement property : properties) {

				String propertyLabel = property.getLabel();

				if (!FacetEnvironment.PROPERTIES_TO_IGNORE
						.contains(propertyLabel)) {

					Stack<Node> propertyPath = new Stack<Node>();
					// NodeElement target = property.getTarget();

					if (propertyLabel.equals(RDF.NAMESPACE + RDF.TYPE)) {

						Node endpoint = new Node(propertyLabel,
								NodeType.INNER_NODE, NodeContent.TYPE_PROPERTY);

						// endpoint.addRangeExtension(target.getLabel());
						endpoint
								.setFacet(endpoint.makeFacet(propertyLabel,
										FacetType.RDF_PROPERTY_BASED,
										DataType.NOT_SET));
						propertyPath.push(endpoint);

					}
					// data- or object-property
					else {

						boolean isDataProp = m_facetHelper
								.isDataProperty(propertyLabel);

						Node endpoint = new Node(propertyLabel,
								NodeType.INNER_NODE, isDataProp
										? NodeContent.DATA_PROPERTY
										: NodeContent.OBJECT_PROPERTY);

						// endpoint.addRangeExtension(target.getLabel());
						endpoint.setFacet(endpoint.makeFacet(propertyLabel,
								isDataProp
										? FacetType.DATAPROPERTY_BASED
										: FacetType.OBJECT_PROPERTY_BASED,
								DataType.NOT_SET));

						propertyPath.push(endpoint);

						String currentProperty = property.getLabel();

						Node superProperty;

						while ((superProperty = m_facetHelper
								.getSuperProperty(currentProperty)) != null) {

							propertyPath.push(superProperty);
							currentProperty = superProperty.getValue();
						}
					}

					// insert path
					insertPropertyPath(propertyPath, facetTree, endPoints);

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

										Node nodeEndpoint = new Node();
										nodeEndpoint.setValue(object);
										nodeEndpoint
												.setContent(NodeContent.CLASS);

										nodeEndpoint
												.setFacet(nodeEndpoint
														.makeFacet(
																property
																		.getValue(),
																FacetType.RDF_PROPERTY_BASED,
																DataType.NOT_SET));

										classPath.push(nodeEndpoint);

										String key = source_extension
												.getLabel()
												+ property.getValue() + object;

										// ArrayList<String> keyElements = new
										// ArrayList<String>();
										// keyElements.add(source_extension
										// .getLabel());
										// keyElements.add(property.getValue());
										// keyElements.add(object);

										String currentClass = object;
										Node superClass;

										while ((superClass = m_facetHelper
												.getSuperClass(currentClass)) != null) {

											superClass
													.setFacet(superClass
															.makeFacet(
																	property
																			.getValue(),
																	FacetType.RDF_PROPERTY_BASED,
																	DataType.NOT_SET));

											classPath.push(superClass);
											currentClass = superClass
													.getValue();

											// keyElements.add(superClass
											// .getValue());
											key += superClass.getValue();
										}

										// String key = FacetDbUtils
										// .getKey(keyElements
										// .toArray(new String[keyElements
										// .size()]));

										Stack<Node> cachedNodes = null;

										if ((cachedNodes = cache.get(key)) == null) {

											cachedNodes = insertClassPath(
													classPath, facetTree,
													property);

											cache.put(key, cachedNodes);
										}

										updateLeaveDB(cachedNodes,
												source_extension.getLabel(),
												individual);

										updateObjectDB(facetTree, cachedNodes,
												individual, object,
												source_extension.getLabel());

									} else if (property.getContent() == NodeContent.OBJECT_PROPERTY) {

										String classLabel = m_facetHelper
												.getClass(object);

										if (classLabel != null) {

											Stack<Node> classPath = new Stack<Node>();

											Node nodeEndpoint = new Node();
											nodeEndpoint.setValue(classLabel);
											nodeEndpoint
													.setContent(NodeContent.CLASS);

											nodeEndpoint
													.setFacet(nodeEndpoint
															.makeFacet(
																	property
																			.getValue(),
																	FacetType.OBJECT_PROPERTY_BASED,
																	DataType.NOT_SET));

											classPath.push(nodeEndpoint);

											// ArrayList<String> keyElements =
											// new ArrayList<String>();
											// keyElements.add(source_extension
											// .getLabel());
											// keyElements
											// .add(property.getValue());
											// keyElements.add(classLabel);

											String key = source_extension
													.getLabel()
													+ property.getValue()
													+ classLabel;

											String currentClass = classLabel;
											Node superClass;

											while ((superClass = m_facetHelper
													.getSuperClass(currentClass)) != null) {

												superClass
														.setFacet(superClass
																.makeFacet(
																		property
																				.getValue(),
																		FacetType.OBJECT_PROPERTY_BASED,
																		DataType.NOT_SET));

												classPath.push(superClass);
												currentClass = superClass
														.getValue();

												// keyElements.add(superClass
												// .getValue());

												key += superClass.getValue();
											}

											// String key = FacetDbUtils
											// .getKey(keyElements
											// .toArray(new String[keyElements
											// .size()]));

											Stack<Node> cachedNodes = null;

											if ((cachedNodes = cache.get(key)) == null) {

												// insert path
												cachedNodes = insertClassPath(
														classPath, facetTree,
														property);

												// FacetDbUtils.store(m_cacheDB,
												// key, cachedNodes);
												cache.put(key, cachedNodes);
											}

											updateLeaveDB(
													cachedNodes,
													source_extension.getLabel(),
													individual);

											updateObjectDB(facetTree,
													cachedNodes, individual,
													object, source_extension
															.getLabel());

											// updateVectorDB(
											// cachedNodes
											// .get(FacetEnvironment.SOURCE),
											// source_extension.getLabel(),
											// extensionSize, individual);

										} else {

											// ArrayList<String> keyElements =
											// new ArrayList<String>();
											// keyElements.add(source_extension
											// .getLabel());
											// keyElements
											// .add(property.getValue());
											// keyElements.add(object);

											String key = source_extension
													.getLabel()
													+ property.getValue()
													+ object;

											// String key = FacetDbUtils
											// .getKey(keyElements
											// .toArray(new String[keyElements
											// .size()]));

											Stack<Node> cachedNodes = null;

											if ((cachedNodes = cache.get(key)) == null) {

												// insert object
												cachedNodes = insertObject(
														facetTree, property,
														object);

												// FacetDbUtils.store(m_cacheDB,
												// key, cachedNodes);
												cache.put(key, cachedNodes);
											}

											updateLeaveDB(
													cachedNodes,
													source_extension.getLabel(),
													individual);

											updateObjectDB(facetTree,
													cachedNodes, individual,
													object, source_extension
															.getLabel());

											// updateVectorDB(
											// cachedNodes
											// .get(FacetEnvironment.SOURCE),
											// source_extension.getLabel(),
											// extensionSize, individual);

										}
									}
								}
								// DataPropery
								else {

									String key = source_extension.getLabel()
											+ property.getValue() + object;

									// String key = FacetDbUtils
									// .getKey(new String[]{
									// source_extension.getLabel(),
									// property.getValue(), object});

									Stack<Node> cachedNodes = null;

									if ((cachedNodes = cache.get(key)) == null) {

										// insert object
										cachedNodes = insertObject(facetTree,
												property, object);

										// FacetDbUtils.store(m_cacheDB,
										// cacheKey,
										// cachedNodes);
										cache.put(key, cachedNodes);
									}

									updateLeaveDB(cachedNodes, source_extension
											.getLabel(), individual);

									updateObjectDB(facetTree, cachedNodes,
											individual, object,
											source_extension.getLabel());
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
			}

			s_log.debug("finished facet tree for extension: "
					+ source_extension + "!");

			Set<Node> leaves = facetTree.getVertex(NodeType.LEAVE);

			for (Node leave : leaves) {

				Queue<Edge> path2root = facetTree.getAncestorPath2Root(leave
						.getID());
				Queue<Edge> path2rangeRoot = facetTree
						.getAncestorPath2RangeRoot(leave.getID());

				if (!FacetDbUtils.contains(m_pathDB,
						FacetEnvironment.Keys.RANGEROOT_PATH
								+ leave.getPathHashValue(), m_pathBinding)) {

					FacetDbUtils.store(m_pathDB,
							FacetEnvironment.Keys.RANGEROOT_PATH
									+ leave.getPathHashValue(), path2rangeRoot,
							m_pathBinding);

					FacetDbUtils.store(m_pathDB,
							FacetEnvironment.Keys.ROOT_PATH
									+ leave.getPathHashValue(), path2root,
							m_pathBinding);
				}
			}
		}
	}

	public void close() throws DatabaseException {

		if (m_pathDB != null) {
			m_pathDB.close();
		}

		if (m_leaveDB != null) {
			m_leaveDB.close();
		}

		if (m_objectDB != null) {
			m_objectDB.close();
		}
		if (m_env != null) {
			m_env.close();
		}
		if (m_env2 != null) {
			m_env2.close();
		}
	}

	private String getPath4Node(FacetTree tree, Node node) {

		boolean reachedRoot = node.isRoot();

		Node currentNode = node;
		String path = "";

		while (!reachedRoot) {

			Iterator<Edge> incomingEdgesIter = tree
					.incomingEdgesOf(currentNode).iterator();

			if (incomingEdgesIter.hasNext()) {

				Node father = tree.getEdgeSource(incomingEdgesIter.next());
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

		path = path + node.getValue();

		return path;
	}

	private void init() throws EnvironmentLockedException, DatabaseException,
			IOException {

		// tree dir ...
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		m_env = new Environment(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true), envConfig);

		// objects dir ...
		m_env2 = new Environment(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_OBJECTS_DIR, true), envConfig);

		// Databases without duplicates
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setDeferredWrite(true);

		// Databases with duplicates
		DatabaseConfig dbConfig2 = new DatabaseConfig();
		dbConfig2.setTransactional(false);
		dbConfig2.setAllowCreate(true);
		dbConfig2.setSortedDuplicates(true);
		dbConfig2.setDeferredWrite(true);

		// Databases without duplicates
		m_pathDB = m_env.openDatabase(null, FacetEnvironment.DatabaseName.PATH,
				dbConfig);

		// Databases with duplicates
		m_leaveDB = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.LEAVE, dbConfig2);

		m_objectDB = m_env2.openDatabase(null,
				FacetEnvironment.DatabaseName.OBJECT, dbConfig2);

		// Create the bindings

		m_pathBinding = new PathBinding();
		m_fvBinding = new AbstractSingleFacetValueBinding();
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_nodeBinding = new NodeBinding();

		m_objectMap = new StoredMap<String, AbstractSingleFacetValue>(
				m_objectDB, m_strgBinding, m_fvBinding, true);

		m_leaveMap = new StoredMap<String, Node>(m_objectDB, m_strgBinding,
				m_nodeBinding, true);
	}

	private Stack<Node> insertClassPath(Stack<Node> classPath,
			FacetTree facetTree, Node endpoint) throws DatabaseException,
			IOException {

		// init
		Stack<Node> nodes2cache = new Stack<Node>();

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

					// nodes2cache.get(FacetEnvironment.SOURCE).add(rangeTop);

					Stack<Edge> rangeEdges = new Stack<Edge>();
					rangeEdges.addAll(facetTree.outgoingEdgesOf(rangeTop));

					if (m_facetHelper.isSubClassOf(rangeTop, classPath.peek())) {
						classPath = pruneClassPath(rangeTop, classPath);
					}

					if (rangeTop.hasSameValueAs(classPath.peek())) {
						classPath.pop();
					}

					if (classPath.isEmpty()) {

						// nodes2cache.get(FacetEnvironment.LEAVE).add(rangeTop);
						nodes2cache.add(rangeTop);

					} else {

						Node topNode = classPath.peek();

						while (!rangeEdges.isEmpty() && !classPath.isEmpty()) {

							Node child = facetTree.getEdgeTarget(rangeEdges
									.pop());

							if (child.hasSameValueAs(topNode)) {

								classPath.pop();

								if (classPath.isEmpty()) {
									nodes2cache.add(child);

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

							Edge edge = facetTree.addEdge(rangeTop, topNode);
							edge.setType(EdgeType.SUBCLASS_OF);

							if (classPath.isEmpty()) {

								nodes2cache.add(topNode);

							} else {

								while (!classPath.isEmpty()) {

									Node tar = classPath.pop();
									facetTree.addVertex(tar);

									edge = facetTree.addEdge(topNode, tar);
									edge.setType(EdgeType.SUBCLASS_OF);

									if (classPath.isEmpty()) {

										nodes2cache.add(tar);
									}

									topNode = tar;
								}
							}
						}
					}
				}
			}
			if (!foundRange) {

				Node rangeTop = m_facetHelper.hasRangeClass(currentNode)
						? m_facetHelper.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(currentNode.getValue()),
								NodeType.RANGE_ROOT, NodeContent.CLASS);

				rangeTop.setFacet(rangeTop.makeFacet(currentNode.getValue(),
						m_facetHelper.isDataProperty(currentNode.getValue())
								? FacetType.DATAPROPERTY_BASED
								: FacetType.OBJECT_PROPERTY_BASED,
						DataType.NOT_SET));

				facetTree.addVertex(rangeTop);

				Edge edge = facetTree.addEdge(currentNode, rangeTop);
				edge.setType(EdgeType.HAS_RANGE);

				if (m_facetHelper.isSubClassOf(rangeTop, classPath.peek())) {
					classPath = pruneClassPath(rangeTop, classPath);
				}

				if (rangeTop.hasSameValueAs(classPath.peek())) {
					classPath.pop();
				}

				if (!classPath.isEmpty()) {

					Node classPathTop = classPath.pop();
					facetTree.addVertex(classPathTop);

					edge = facetTree.addEdge(rangeTop, classPathTop);
					edge.setType(EdgeType.SUBCLASS_OF);

					if (classPath.isEmpty()) {

						nodes2cache.add(classPathTop);

					} else {

						while (!classPath.isEmpty()) {

							Node tar = classPath.pop();
							facetTree.addVertex(tar);

							edge = facetTree.addEdge(classPathTop, tar);
							edge.setType(EdgeType.SUBCLASS_OF);

							if (classPath.isEmpty()) {
								nodes2cache.add(tar);
							}

							classPathTop = tar;
						}
					}
				} else {

					nodes2cache.add(rangeTop);
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

	private Stack<Node> insertObject(FacetTree facetTree, Node endpoint,
			String object) throws DatabaseException, IOException {

		// init
		Stack<Node> nodes2cache = new Stack<Node>();
		// nodes2cache.put(FacetEnvironment.SOURCE, new Stack<Node>());
		// nodes2cache.put(FacetEnvironment.LEAVE, new Stack<Node>());

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
					nodes2cache.add(rangeTop);
				}
			}

			if (!foundRange) {

				Node rangeTop = this.m_facetHelper.hasRangeClass(currentNode)
						? this.m_facetHelper.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(currentNode.getValue()),
								NodeType.RANGE_ROOT, NodeContent.CLASS);

				rangeTop.setFacet(rangeTop.makeFacet(currentNode.getValue(),
						m_facetHelper.isDataProperty(currentNode.getValue())
								? FacetType.DATAPROPERTY_BASED
								: FacetType.OBJECT_PROPERTY_BASED, FacetUtils
								.getLiteralDataType(object)));

				facetTree.addVertex(rangeTop);

				Edge edge = facetTree.addEdge(currentNode, rangeTop);
				edge.setType(EdgeType.HAS_RANGE);

				nodes2cache.add(rangeTop);
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

			if (currentTarget.hasSameValueAs(topNode)) {

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
		Set<Node> leaves = currentTree.getVertex(NodeType.LEAVE);
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

						if ((currentTree.outDegreeOf(father) == 1)) {

							// && (father.isEndPoint()))

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
		Set<Node> leaves = currentTree.getVertex(NodeType.LEAVE);
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

						if ((currentTree.outgoingEdgesOf(father).size() == 1)) {

							// && (father.isEndPoint())

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

	private void updateLeaveDB(Stack<Node> leaves, String extension,
			String individual) throws DatabaseException, IOException {

		String key = extension + individual;

		for (Node leave : leaves) {
			// FacetDbUtils.store(m_leaveDB, key, leave, m_nodeBinding);
			m_leaveMap.put(key, leave);
		}
	}

	private void updateObjectDB(FacetTree tree, Stack<Node> leaves, String ind,
			String object, String sourceExt) throws DatabaseException,
			IOException, StorageException {

		String rangeExt = m_idxReader.getStructureIndex().getExtension(object);

		AbstractSingleFacetValue fv;

		if (Util.isEntity(object)) {

			fv = new Resource();
			((Resource) fv).setValue(object);
			((Resource) fv).setRangeExt(rangeExt);
			((Resource) fv).setSourceExt(sourceExt);
			((Resource) fv).setIsResource(true);

		} else {

			fv = new Literal();
			((Literal) fv).setValue(object);
			((Literal) fv).setRangeExt(rangeExt);
			((Literal) fv).setSourceExt(sourceExt);
			((Literal) fv).setIsResource(false);
		}

		for (Node leave : leaves) {

			if (!leave.hasPath()) {

				String path = getPath4Node(tree, leave);
				leave.setPath(path);
				leave.setPathHashValue(path.hashCode());
			}

			String key4obj = ind + leave.getPathHashValue();
			// FacetDbUtils.store(m_objectDB, key4obj, fv, m_fvBinding);
			m_objectMap.put(key4obj, fv);
		}
	}
}
