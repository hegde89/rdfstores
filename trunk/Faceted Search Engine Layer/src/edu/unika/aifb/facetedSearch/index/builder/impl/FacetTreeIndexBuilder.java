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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

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
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.index.db.binding.NodeBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.PathBinding;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
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

	private final static Logger s_log = Logger
			.getLogger(FacetTreeIndexBuilder.class);

	/*
	 * 
	 */
	private IndexReader m_idxReader;

	/*
	 * 
	 */
	private Environment m_env;

	/*
	 * 
	 */
	private Database m_pathDB;
	private Database m_leaveDB;

	/*
	 * 
	 */
	private EntryBinding<Queue<Edge>> m_pathBinding;
	private EntryBinding<Node> m_nodeBinding;
	private EntryBinding<String> m_strgBinding;

	/*
	 * 
	 */
	private StoredMap<String, Node> m_leaveMap;

	/*
	 * 
	 */
	private FacetIdxBuilderHelper m_facetHelper;

	/*
	 * 
	 */
	private File m_dir;

	/*
	 * 
	 */
	private HashMap<String, HashSet<String>> m_property2subjectMap;
	private HashMap<String, HashSet<String>> m_propertySubject2objectMap;

	public FacetTreeIndexBuilder(IndexReader idxReader,
			FacetIdxBuilderHelper helper, File dir)
			throws EnvironmentLockedException, DatabaseException, IOException {

		m_facetHelper = helper;
		m_idxReader = idxReader;
		m_dir = dir;

		init();
	}

	public void build() throws IOException, StorageException, DatabaseException {

		HashSet<String> leaveDbCache = new HashSet<String>();
		HashMap<String, Stack<Node>> cache = new HashMap<String, Stack<Node>>();
		Set<NodeElement> source_extensions = m_facetHelper.getIndexGraph()
				.vertexSet();

		int count_ext = 0;

		for (NodeElement source_extension : source_extensions) {

			cache.clear();
			leaveDbCache.clear();

			s_log.debug("start building facet tree for extension: "
					+ source_extension + " (" + (++count_ext) + "/"
					+ source_extensions.size() + ")");

			FacetTree facetTree = new FacetTree();

			int count_subject = findAllProperties(source_extension.getLabel());
			Set<String> properties = m_property2subjectMap.keySet();
			Set<Node> mostSpecificProperties = new HashSet<Node>();

			// get property-paths
			for (String property : properties) {

				Stack<Node> propertyPath = new Stack<Node>();

				if (property.equals(RDF.NAMESPACE + RDF.TYPE)) {

					Node endpoint = new Node(property, NodeType.INNER_NODE,
							NodeContent.TYPE_PROPERTY);

					Facet facet = new Facet(property,
							FacetType.RDF_PROPERTY_BASED, DataType.NOT_SET);

					facet.setLabel(m_facetHelper.getLabel(property));
					endpoint.setFacet(facet);

					mostSpecificProperties.add(endpoint);
					propertyPath.push(endpoint);

				}
				// data- or object-property
				else {

					boolean isDataProp = m_facetHelper.isDataProperty(property);

					Node endpoint = new Node(property, NodeType.INNER_NODE,
							isDataProp
									? NodeContent.DATA_PROPERTY
									: NodeContent.OBJECT_PROPERTY);

					Facet facet = new Facet(property, isDataProp
							? FacetType.DATAPROPERTY_BASED
							: FacetType.OBJECT_PROPERTY_BASED, DataType.NOT_SET);

					facet.setDataType(m_facetHelper.getDataType(property));
					facet.setLabel(m_facetHelper.getLabel(property));

					endpoint.setFacet(facet);

					mostSpecificProperties.add(endpoint);
					propertyPath.push(endpoint);

					// String currentProperty = property;
					// Node superProperty;

					// while ((superProperty = m_facetHelper
					// .getSuperProperty(currentProperty)) != null) {
					//
					// propertyPath.push(superProperty);
					// currentProperty = superProperty.getValue();
					// }
				}

				// insert path
				insertPropertyPath(propertyPath, facetTree);
			}

			if (!facetTree.isEmpty()) {

				s_log.debug("inserted properties in facetTree!");
				s_log.debug("start going over endpoints... ");

				s_log.debug("extension contains " + count_subject
						+ " individuals.");

				int count_prop = 0;
				int total_prop_size = mostSpecificProperties.size();

				Iterator<Node> propertyIter = mostSpecificProperties.iterator();

				while (propertyIter.hasNext()) {

					Node property = propertyIter.next();

					Set<String> individuals = m_property2subjectMap
							.get(property.getValue());

					s_log.debug("going over property " + property.getValue()
							+ ". " + (++count_prop) + "/" + total_prop_size);

					for (String individual : individuals) {

						try {

							Iterator<String> objectIter = m_propertySubject2objectMap
									.get(property.getValue() + individual)
									.iterator();

							while (objectIter.hasNext()) {

								String object = objectIter.next();

								if (property.getContent() != NodeContent.DATA_PROPERTY) {

									if (property.getContent() == NodeContent.TYPE_PROPERTY) {

										if (!m_facetHelper.hasSubClass(object)) {

											s_log.debug("inserted: "
													+ individual + " type "
													+ object);

											Node nodeEndpoint = new Node();
											nodeEndpoint.setValue(object);
											nodeEndpoint
													.setContent(NodeContent.CLASS);

											Facet facet = new Facet(
													property.getValue(),
													FacetType.RDF_PROPERTY_BASED,
													DataType.NOT_SET);

											facet.setLabel(m_facetHelper
													.getLabel(property
															.getValue()));
											nodeEndpoint.setFacet(facet);

											String key = source_extension
													.getLabel()
													+ property.getValue()
													+ object;

											Stack<Node> leaves = null;

											if ((leaves = cache.get(key)) == null) {

												Stack<Node> classPath = new Stack<Node>();
												classPath.push(nodeEndpoint);

												String currentClass = object;
												Node superClass;

												while ((superClass = m_facetHelper
														.getSuperClass(currentClass)) != null) {

													superClass.setFacet(facet);
													classPath.push(superClass);

													currentClass = superClass
															.getValue();
												}

												leaves = insertClassPath(
														classPath, facetTree,
														property);

												for (Node leave : leaves) {

													Queue<Edge> path2root = getAncestorPath2Root(
															facetTree, leave);

													FacetDbUtils.store(
															m_pathDB, leave
																	.getPath(),
															path2root,
															m_pathBinding);
												}

												cache.put(key, leaves);
											}

											updateLeaveDB(leaves, individual,
													facetTree, leaveDbCache);
										}
									} else if (property.getContent() == NodeContent.OBJECT_PROPERTY) {

										String classLabel = m_facetHelper
												.getClass(object);

										if (classLabel != null) {

											Node nodeEndpoint = new Node();
											nodeEndpoint.setValue(classLabel);
											nodeEndpoint
													.setContent(NodeContent.CLASS);

											Facet facet = new Facet(
													property.getValue(),
													FacetType.OBJECT_PROPERTY_BASED,
													DataType.NOT_SET);
											facet.setLabel(m_facetHelper
													.getLabel(property
															.getValue()));
											nodeEndpoint.setFacet(facet);

											String key = source_extension
													.getLabel()
													+ property.getValue()
													+ classLabel;

											Stack<Node> leaves = null;

											if ((leaves = cache.get(key)) == null) {

												Stack<Node> classPath = new Stack<Node>();
												classPath.push(nodeEndpoint);

												String currentClass = classLabel;
												Node superClass;

												while ((superClass = m_facetHelper
														.getSuperClass(currentClass)) != null) {

													superClass.setFacet(facet);

													classPath.push(superClass);
													currentClass = superClass
															.getValue();
												}

												// insert path
												leaves = insertClassPath(
														classPath, facetTree,
														property);

												for (Node leave : leaves) {

													Queue<Edge> path2root = getAncestorPath2Root(
															facetTree, leave);

													FacetDbUtils.store(
															m_pathDB, leave
																	.getPath(),
															path2root,
															m_pathBinding);
												}

												cache.put(key, leaves);
											}

											updateLeaveDB(leaves, individual,
													facetTree, leaveDbCache);

										} else {

											String key = source_extension
													.getLabel()
													+ property.getValue();

											Stack<Node> leaves = null;

											if ((leaves = cache.get(key)) == null) {

												// insert object
												leaves = insertObject(
														facetTree, property,
														object);

												for (Node leave : leaves) {

													Queue<Edge> path2root = getAncestorPath2Root(
															facetTree, leave);

													FacetDbUtils.store(
															m_pathDB, leave
																	.getPath(),
															path2root,
															m_pathBinding);
												}

												cache.put(key, leaves);
											}

											updateLeaveDB(leaves, individual,
													facetTree, leaveDbCache);
										}
									}
								}
								// DataPropery
								else {

									String key = source_extension.getLabel()
											+ property.getValue();

									Stack<Node> leaves = null;

									if ((leaves = cache.get(key)) == null) {

										// insert object
										leaves = insertObject(facetTree,
												property, object);

										for (Node leave : leaves) {

											Queue<Edge> path2root = getAncestorPath2Root(
													facetTree, leave);

											FacetDbUtils.store(m_pathDB, leave
													.getPath(), path2root,
													m_pathBinding);
										}

										cache.put(key, leaves);
									}

									updateLeaveDB(leaves, individual,
											facetTree, leaveDbCache);

								}
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (NullPointerException e) {
							e.printStackTrace();
						}

						m_propertySubject2objectMap.get(
								property.getValue() + individual).clear();
					}

					cache.clear();
					leaveDbCache.clear();
					m_property2subjectMap.get(property.getValue()).clear();
				}
			}

			if (!treeIsValid(facetTree, facetTree.getVertices(NodeType.LEAVE))) {

				s_log.error("finished facet tree for extension: "
						+ source_extension + " not valid!");

				System.out.println("tree : " + facetTree);
				break;
			}

			s_log.debug("finished facet tree for extension: "
					+ source_extension + "!");
		}
	}

	public void close() throws DatabaseException {

		if (m_pathDB != null) {
			m_pathDB.close();
		}
		if (m_leaveDB != null) {
			m_leaveDB.close();
		}
		if (m_env != null) {
			m_env.close();
		}
	}

	private int findAllProperties(String ext) throws IOException,
			StorageException {

		boolean log = ext.equals("b41373");

		m_property2subjectMap.clear();
		m_propertySubject2objectMap.clear();

		IndexStorage spIdx = m_idxReader.getStructureIndex()
				.getSPIndexStorage();
		List<String> individuals = spIdx.getDataList(IndexDescription.EXTENT,
				DataField.ENT, ext);

		if (log) {
			s_log.debug("individuals.size(): " + individuals.size());
		}

		int subject_count = individuals.size();
		int count = 0;

		for (String individual : individuals) {

			if (log) {
				s_log.debug("count: " + (++count) + " / " + subject_count);
			}

			Table<String> triples = m_idxReader.getDataIndex().getTriples(
					individual, null, null);

			Iterator<String[]> tripleIter = triples.getRows().iterator();

			while (tripleIter.hasNext()) {

				String[] triple = tripleIter.next();

				String prop = triple[1];
				String object = triple[2];

				if (log) {
					s_log.debug("individual: " + individual + " // prop: "
							+ prop + " // object: " + object);
				}

				if (!FacetEnvironment.PROPERTIES_TO_IGNORE.contains(prop)
						&& !prop.startsWith(FacetEnvironment.OWL.NAMESPACE)) {

					if (prop.equals(FacetEnvironment.RDF.NAMESPACE
							+ FacetEnvironment.RDF.TYPE)) {

						if (!object.startsWith(FacetEnvironment.OWL.NAMESPACE)
								&& !object
										.startsWith(FacetEnvironment.RDF.NAMESPACE)
								&& !object
										.startsWith(FacetEnvironment.RDFS.NAMESPACE)) {

							if (!m_property2subjectMap.containsKey(prop)) {
								m_property2subjectMap.put(prop,
										new HashSet<String>());
							}

							HashSet<String> subjects = m_property2subjectMap
									.get(prop);
							subjects.add(individual);

							m_property2subjectMap.put(prop, subjects);

							if (!m_propertySubject2objectMap.containsKey(prop
									+ individual)) {
								m_propertySubject2objectMap.put(prop
										+ individual, new HashSet<String>());
							}

							HashSet<String> objects = m_propertySubject2objectMap
									.get(prop + individual);
							objects.add(object);

							m_propertySubject2objectMap.put(prop + individual,
									objects);
						}
					} else {

						if (!m_property2subjectMap.containsKey(prop)) {
							m_property2subjectMap.put(prop,
									new HashSet<String>());
						}

						HashSet<String> subjects = m_property2subjectMap
								.get(prop);
						subjects.add(individual);

						m_property2subjectMap.put(prop, subjects);

						if (!m_propertySubject2objectMap.containsKey(prop
								+ individual)) {
							m_propertySubject2objectMap.put(prop + individual,
									new HashSet<String>());
						}

						HashSet<String> objects = m_propertySubject2objectMap
								.get(prop + individual);
						objects.add(object);

						m_propertySubject2objectMap.put(prop + individual,
								objects);
					}
				}
			}
		}

		if (log) {
			s_log.debug("finished findAllProperties()");
		}

		return subject_count;
	}

	private LinkedList<Edge> getAncestorPath2Root(FacetTree tree, Node leave) {
		Node currentNode = leave;
		boolean reachedRoot = leave.isRoot();

		LinkedList<Edge> edges2root = new LinkedList<Edge>();

		while (!reachedRoot) {

			Iterator<Edge> incomingEdgesIter = tree
					.incomingEdgesOf(currentNode).iterator();

			if (incomingEdgesIter.hasNext()) {

				Edge edge2father = incomingEdgesIter.next();

				if (!edge2father.getTarget().hasPath()) {
					edge2father.getTarget().updatePath(tree);
				}

				if (!edge2father.getSource().hasPath()) {
					edge2father.getSource().updatePath(tree);
				}

				edges2root.add(edge2father);
				Node father = tree.getEdgeSource(edge2father);

				if (father.isRoot()) {
					reachedRoot = true;
				} else {
					currentNode = father;
				}
			} else {
				s_log.error("tree structure is not correct " + this);
				break;
			}
		}

		return edges2root;
	}

	private void init() throws EnvironmentLockedException, DatabaseException,
			IOException {

		m_property2subjectMap = new HashMap<String, HashSet<String>>();
		m_propertySubject2objectMap = new HashMap<String, HashSet<String>>();

		// tree dir ...
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		m_env = new Environment(m_dir, envConfig);

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

		// Create the bindings
		m_pathBinding = new PathBinding();
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_nodeBinding = new NodeBinding();

		m_leaveMap = new StoredMap<String, Node>(m_leaveDB, m_strgBinding,
				m_nodeBinding, true);
	}

	private Stack<Node> insertClassPath(Stack<Node> classPath,
			FacetTree facetTree, Node endpoint) throws DatabaseException,
			IOException {

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

					Stack<Edge> rangeEdges = new Stack<Edge>();
					rangeEdges.addAll(facetTree.outgoingEdgesOf(rangeTop));

					if (m_facetHelper.isSubClassOf(rangeTop, classPath.peek())) {
						classPath = pruneClassPath(rangeTop, classPath);
					}

					if (rangeTop.hasSameValueAs(classPath.peek())) {
						classPath.pop();
					}

					if (classPath.isEmpty()) {
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

				Facet facet = new Facet(currentNode.getValue(), m_facetHelper
						.isDataProperty(currentNode.getValue())
						? FacetType.DATAPROPERTY_BASED
						: FacetType.OBJECT_PROPERTY_BASED, DataType.NOT_SET);

				facet.setDataType(m_facetHelper.getDataType(currentNode
						.getValue()));
				facet.setLabel(m_facetHelper.getLabel(currentNode.getValue()));

				rangeTop.setFacet(facet);

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
				s_log.error("tree structure is not correct: " + facetTree);
				break;
			}
		} while (!reachedRoot);

		return nodes2cache;
	}

	private Stack<Node> insertObject(FacetTree facetTree, Node endpoint,
			String object) throws DatabaseException, IOException {

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
					nodes2cache.add(rangeTop);
				}
			}

			if (!foundRange) {

				Node rangeTop = m_facetHelper.hasRangeClass(currentNode)
						? m_facetHelper.getRange(currentNode)
						: new Node("Generic Range: "
								+ Util.truncateUri(currentNode.getValue()),
								NodeType.RANGE_ROOT, NodeContent.CLASS);

				Facet facet = new Facet(currentNode.getValue(), m_facetHelper
						.isDataProperty(currentNode.getValue())
						? FacetType.DATAPROPERTY_BASED
						: FacetType.OBJECT_PROPERTY_BASED, FacetUtils
						.getLiteralDataType(object));

				facet.setDataType(m_facetHelper.getDataType(currentNode
						.getValue()));
				facet.setLabel(m_facetHelper.getLabel(currentNode.getValue()));

				rangeTop.setFacet(facet);

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

	private void insertPropertyPath(Stack<Node> path, FacetTree facetTree) {

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

			while (!path.isEmpty()) {

				Node tar = path.pop();
				facetTree.addVertex(tar);

				edge = facetTree.addEdge(topNode, tar);
				edge.setType(EdgeType.SUBPROPERTY_OF);

				topNode = tar;
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
		Set<Node> leaves = currentTree.getVertices(NodeType.LEAVE);
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
		Set<Node> leaves = currentTree.getVertices(NodeType.LEAVE);
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

	private boolean treeIsValid(FacetTree tree, Set<Node> leaves) {

		Iterator<Node> iter = leaves.iterator();

		while (iter.hasNext()) {

			Node currentNode = iter.next();
			boolean reachedRoot = currentNode.isRoot();

			// walk to root

			while (!reachedRoot) {

				Iterator<Edge> incomingEdgesIter = tree.incomingEdgesOf(
						currentNode).iterator();

				if (currentNode.isRoot()) {

					reachedRoot = true;

				} else if (incomingEdgesIter.hasNext()) {

					Edge edge2father = incomingEdgesIter.next();
					Node father = tree.getEdgeSource(edge2father);

					if (father.isRoot()) {

						reachedRoot = true;

					} else {

						if (currentNode.getPath() == null) {

							s_log.error("currentNode path is not set:"
									+ currentNode);
							return false;
						}

						if (currentNode.getContent() == 0) {
							s_log.error("currentNode content is not set:"
									+ currentNode);
							return false;
						}

						if (currentNode.getValue() == null) {
							s_log.error("currentNode value is null:"
									+ currentNode);
							return false;
						}

						currentNode = father;

					}
				} else {
					return false;
				}
			}
		}

		return true;
	}

	private void updateLeaveDB(Stack<Node> leaves, String individual,
			FacetTree tree, HashSet<String> cache) throws DatabaseException,
			IOException {

		for (Node leave : leaves) {

			if (!cache.contains(leave.getID() + individual)) {

				if (!leave.hasPath()) {
					leave.updatePath(tree);
				}

				m_leaveMap.put(individual, leave);
				cache.add(leave.getID() + individual);
			}
		}
	}
}
