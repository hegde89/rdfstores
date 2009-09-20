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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.PreloadConfig;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.api.model.impl.Facet.FacetType;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeType;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class FacetIndexHelper {

	// private static LuceneIndexStorage s_vPosIndex;
	private static StructureIndex s_structureIndex;
	private static IndexDirectory s_idxDirectory;
	private static IndexReader s_idxReader;
	private static Environment s_env;
	private static Database s_cacheDB;
	private static HashSet<String> s_classes;
	private static FacetIndexHelper s_instance;
	private static ArrayList<String> s_objectProperties;
	private static ArrayList<String> s_dataProperties;
	// private static Database s_literalDB;

	/*
	 * Indices
	 */
	private static Database s_leaveDB;
	// private static Database s_endpointDB;

	/*
	 * Bindings
	 */
	private static SerialBinding<String> s_stringBinding;
	private static SerialBinding<Node> s_nodeBinding;

	private static DirectedMultigraph<NodeElement, EdgeElement> s_indexGraph;

	public static void close() throws DatabaseException, StorageException,
			IOException {

		// s_vPosIndex.close();
		s_structureIndex.close();
		s_cacheDB.close();

		s_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FH_CACHE);
		s_idxDirectory.getDirectory(IndexDirectory.FACET_TEMP_DIR).delete();

		s_classes = null;
		s_objectProperties = null;
		s_dataProperties = null;
		s_instance = null;

		System.gc();
	}

	public static FacetIndexHelper getInstance(IndexReader idxReader,
			IndexDirectory idxDirectory) throws EnvironmentLockedException,
			DatabaseException, IOException, StorageException {

		return s_instance == null ? s_instance = new FacetIndexHelper(
				idxReader, idxDirectory) : s_instance;
	}

	private FacetIndexHelper(IndexReader idxReader, IndexDirectory idxDirectory)
			throws EnvironmentLockedException, DatabaseException, IOException,
			StorageException {

		s_idxDirectory = idxDirectory;
		s_idxReader = idxReader;
		s_structureIndex = s_idxReader.getStructureIndex();

		s_objectProperties = new ArrayList<String>();
		s_dataProperties = new ArrayList<String>();

		initGraphIndex(idxReader);
		initClasses(idxReader);
		initCache(idxDirectory);

	}

	public String getClass(String individual) throws DatabaseException,
			IOException {

		String clazz = null;

		if ((clazz = FacetDbUtils.get(s_cacheDB, "class_" + individual,
				s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader.getDataIndex().getTriples(
						individual,
						FacetEnvironment.RDF.NAMESPACE
								+ FacetEnvironment.RDF.TYPE, null);

				if (!triples.getRows().isEmpty()) {
					clazz = triples.getRow(0)[2];
				}
			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (clazz != null) {
				FacetDbUtils.store(s_cacheDB, "class_" + individual, clazz,
						s_stringBinding);
			}
		}

		return clazz;
	}

	// public HashMap<Node, HashSet<String>> getEndPoints(String extension)
	// throws StorageException, IOException, DatabaseException {
	//
	// return FacetDbUtils.get(s_endpointDB, extension);
	// }

	public String getExtension(String object) throws StorageException,
			IOException, DatabaseException {

		String extension = null;

		if ((extension = FacetDbUtils.get(s_cacheDB, "ex_" + object,
				s_stringBinding)) == null) {

			extension = s_structureIndex.getExtension(object);

			if (extension != null) {
				FacetDbUtils.store(s_cacheDB, "ex_" + object, extension,
						s_stringBinding);
			}
		}

		return extension;
	}

	public DirectedMultigraph<NodeElement, EdgeElement> getIndexGraph() {
		return s_indexGraph;
	}

	public HashSet<Node> getLeaves(String extension, String object)
			throws DatabaseException, IOException {

		return FacetDbUtils.getAllAsSet(s_leaveDB, FacetDbUtils
				.getKey(new String[] { extension, object }), s_nodeBinding);
	}

	// public LiteralList getLiterals(String extension, String property)
	// throws DatabaseException, IOException {
	//
	// return (LiteralList) FacetDbUtils.get(s_literalDB, FacetDbUtils
	// .getKey(new String[] { extension, property }));
	// }

	// public int getPosition(String extension, String subject)
	// throws IOException, StorageException {
	//
	// if (s_vPosIndex == null) {
	//
	// s_vPosIndex = new LuceneIndexStorage(s_idxDirectory.getDirectory(
	// IndexDirectory.FACET_VPOS_DIR, false),
	// new StatisticsCollector());
	//
	// s_vPosIndex.initialize(true, true);
	//
	// }
	//
	// String posString = s_vPosIndex.getDataItem(IndexDescription.ESV,
	// DataField.VECTOR_POS, new String[] { extension, subject });
	//
	// return Integer.parseInt(posString);
	// }

	// /**
	// * @return the propEndPointDB
	// */
	// public Database getPropEndPointDB() {
	// return s_propEndPointDB;
	// }

	// /**
	// * @return the propEndPointDB
	// * @throws IOException
	// * @throws DatabaseException
	// */
	// @SuppressWarnings("unchecked")
	// public HashMap<Node, HashSet<String>> getPropEndPoints(String extension)
	// throws DatabaseException, IOException {
	//
	// return (HashMap<Node, HashSet<String>>) FacetDbUtils.get(
	// s_propEndPointDB, extension);
	// }

	public Node getRange(Node property) throws DatabaseException, IOException {

		String rangeClassLabel = null;

		if ((rangeClassLabel = FacetDbUtils.get(s_cacheDB, "range_"
				+ property.getValue(), s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader.getDataIndex().getTriples(
						property.getValue(),
						FacetEnvironment.RDFS.NAMESPACE
								+ FacetEnvironment.RDFS.HAS_RANGE, null);

				if (!triples.getRows().isEmpty()) {
					rangeClassLabel = triples.getRow(0)[2];
				}
			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (rangeClassLabel != null) {
				FacetDbUtils.store(s_cacheDB, "range_" + property.getValue(),
						rangeClassLabel, s_stringBinding);
			}
		}

		return rangeClassLabel == null ? null : new Node(rangeClassLabel,
				NodeType.RANGE_ROOT, NodeContent.CLASS);
	}

	public Node getSuperClass(String clazz) throws DatabaseException,
			IOException {

		String superClass = null;

		if ((superClass = FacetDbUtils.get(s_cacheDB, "superClass_" + clazz,
				s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader.getDataIndex().getTriples(
						clazz,
						FacetEnvironment.RDFS.NAMESPACE
								+ FacetEnvironment.RDFS.SUBCLASS_OF, null);

				if (!triples.getRows().isEmpty()) {
					superClass = triples.getRow(0)[2];
				}
			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (superClass != null) {
				FacetDbUtils.store(s_cacheDB, "superClass_" + clazz,
						superClass, s_stringBinding);
			}
		}

		if (superClass == null) {
			return null;
		} else {

			Node superClassNode = new Node(superClass, NodeType.INNER_NODE,
					NodeContent.CLASS);

			return superClassNode;
		}
	}

	public Node getSuperProperty(String property) throws IOException,
			DatabaseException {

		String superProperty = null;
		FacetType ftype = isDataProperty(property) ? FacetType.DATAPROPERTY_BASED
				: FacetType.OBJECT_PROPERTY_BASED;

		if ((superProperty = FacetDbUtils.get(s_cacheDB, "superProperty_"
				+ property, s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader.getDataIndex().getTriples(
						property,
						FacetEnvironment.RDFS.NAMESPACE
								+ FacetEnvironment.RDFS.SUBPROPERTY_OF, null);

				if (!triples.getRows().isEmpty()) {
					superProperty = triples.getRow(0)[2];
				}
			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (superProperty != null) {
				FacetDbUtils.store(s_cacheDB, "superProperty_" + property,
						superProperty, s_stringBinding);
			}
		}

		if (superProperty == null) {
			return null;
		} else {

			Node superPropertyNode = new Node(superProperty, this
					.isDataProperty(property) ? NodeContent.DATA_PROPERTY
					: NodeContent.OBJECT_PROPERTY);

			superPropertyNode.setFacet(superPropertyNode.makeFacet(
					superProperty, ftype, null));

			return superPropertyNode;
		}
	}

	public boolean hasRangeClass(Node property) throws DatabaseException,
			IOException {

		return getRange(property) == null ? false : true;
	}

	private void initCache(IndexDirectory idxDirectory)
			throws EnvironmentLockedException, DatabaseException, IOException {

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		s_env = new Environment(idxDirectory.getDirectory(
				IndexDirectory.FACET_TEMP_DIR, true), envConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);

		s_cacheDB = s_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FH_CACHE, config);

		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(FacetEnvironment.DefaultValue.PRELOAD_TIME);
		s_cacheDB.preload(pc);

		// Create the bindings
		Database classDb = s_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.CLASS, config);
		StoredClassCatalog cata = new StoredClassCatalog(classDb);

		s_stringBinding = new SerialBinding<String>(cata, String.class);
		s_nodeBinding = new SerialBinding<Node>(cata, Node.class);

	}

	private void initClasses(IndexReader idxReader) {

		try {

			s_classes = new HashSet<String>();

			Table<String> triples = idxReader.getDataIndex().getTriples(null,
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
					"http://www.w3.org/2000/01/rdf-schema#Class");

			Iterator<String[]> rowIter = triples.getRows().iterator();

			while (rowIter.hasNext()) {

				String[] row = rowIter.next();
				s_classes.add(row[0]);
			}
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}

	private void initGraphIndex(IndexReader idxReader) {

		s_indexGraph = new DirectedMultigraph<NodeElement, EdgeElement>(
				EdgeElement.class);

		try {

			IndexStorage gs = idxReader.getStructureIndex()
					.getGraphIndexStorage();

			Set<String> props = idxReader.getObjectProperties();

			for (String property : props) {

				Table<String> table = gs.getIndexTable(IndexDescription.POS,
						DataField.SUBJECT, DataField.OBJECT, property);

				s_objectProperties.add(property);

				for (String[] row : table) {

					String src = row[0];
					String trg = row[1];

					NodeElement sourceNode;
					NodeElement targetNode;

					s_indexGraph.addVertex(sourceNode = new NodeElement(src));
					s_indexGraph.addVertex(targetNode = new NodeElement(trg));

					s_indexGraph.addEdge(sourceNode, targetNode,
							new EdgeElement(sourceNode, property, targetNode));
				}
			}

			props.clear();
			props.addAll(idxReader.getDataProperties());

			for (String property : props) {

				Table<String> table = gs.getIndexTable(IndexDescription.POS,
						DataField.SUBJECT, DataField.OBJECT, property);

				s_dataProperties.add(property);

				for (String[] row : table) {

					String src = row[0];
					String trg = row[1];

					NodeElement sourceNode;
					NodeElement targetNode;

					s_indexGraph.addVertex(sourceNode = new NodeElement(src));
					s_indexGraph.addVertex(targetNode = new NodeElement(trg));

					s_indexGraph.addEdge(sourceNode, targetNode,
							new EdgeElement(sourceNode, property, targetNode));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	public boolean isDataProperty(String prop) throws IOException {
		return s_dataProperties.contains(prop);
	}

	public boolean isObjectProperty(String prop) throws IOException {
		return s_objectProperties.contains(prop);
	}

	/**
	 * 
	 * Tests if class1 is a subclass of class2
	 * 
	 * @param class1
	 * @param class2
	 * @return
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public boolean isSubClassOf(Node class1, Node class2)
			throws DatabaseException, IOException {

		String isSubClass = null;

		if ((isSubClass = FacetDbUtils.get(s_cacheDB, "subClassOf_" + class1
				+ "_" + class2, s_stringBinding)) == null) {

			isSubClass = "0";

			if (class1.hasSameValueAs(class2)) {
				isSubClass = "0";
			} else {

				String currentClass = class1.getValue();
				Node superClass;

				while ((superClass = this.getSuperClass(currentClass)) != null) {

					if (superClass.hasSameValueAs(class2)) {

						isSubClass = "1";
						break;
					} else {
						currentClass = superClass.getValue();
					}
				}
			}

			if (isSubClass != null) {
				FacetDbUtils.store(s_cacheDB, "subClassOf_" + class1 + "_"
						+ class2, isSubClass, s_stringBinding);
			}
		}

		return !isSubClass.equals("0");
	}

	// public void setEndPointDB(Database db) {
	// s_endpointDB = db;
	// }

	public void setLeaveDB(Database db) {
		s_leaveDB = db;
	}

	// public void setLiteralDB(Database db) {
	// s_literalDB = db;
	// }

	// public void setPropertyEndPointDB(Database db) {
	// s_propEndPointDB = db;
	// }

	// public void setVPosIndex(LuceneIndexStorage vPosIndex) {
	// s_vPosIndex = vPosIndex;
	// }

	// public void updateLiterals(LiteralList list, String extension,
	// String property) throws DatabaseException, IOException {
	//
	// FacetDbUtils.store(s_literalDB, FacetDbUtils.getKey(new String[] {
	// extension, property }), list);
	// }
}
