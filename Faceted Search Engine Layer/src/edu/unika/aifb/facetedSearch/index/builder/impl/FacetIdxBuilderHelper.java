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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.Expressivity;
import edu.unika.aifb.facetedSearch.FacetEnvironment.FacetType;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeContent;
import edu.unika.aifb.facetedSearch.FacetEnvironment.NodeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.Facet;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
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

public class FacetIdxBuilderHelper {

	/*
	 * 
	 */
	private static String s_expressivity;

	/*
	 * 
	 */
	private static StructureIndex s_structureIndex;
	private static IndexDirectory s_idxDirectory;
	private static IndexReader s_idxReader;

	/*
	 * 
	 */
	private static Environment s_env;
	private static Database s_cacheDB;

	/*
	 * 
	 */
	private static EntryBinding<String> s_stringBinding;

	/*
	 * 
	 */
	private static HashSet<String> s_classes;

	/*
	 * 
	 */
	private static DirectedMultigraph<NodeElement, EdgeElement> s_indexGraph;

	private static FacetIdxBuilderHelper s_instance;

	public static void close() throws DatabaseException, StorageException,
			IOException {

		if (s_structureIndex != null) {
			s_structureIndex.close();
		}

		if (s_cacheDB != null) {
			s_cacheDB.close();
		}

		if (s_env != null) {
			s_env.removeDatabase(null, FacetEnvironment.DatabaseName.FH_CACHE);
			s_env.close();
		}

		s_idxDirectory.getDirectory(IndexDirectory.FACET_TEMP_DIR).delete();

		s_classes = null;
		s_instance = null;

		System.gc();
	}

	public static FacetIdxBuilderHelper getInstance(IndexReader idxReader,
			IndexDirectory idxDirectory, String expressivity)
			throws EnvironmentLockedException, DatabaseException, IOException,
			StorageException {

		return s_instance == null ? s_instance = new FacetIdxBuilderHelper(
				idxReader, idxDirectory, expressivity) : s_instance;
	}

	private FacetIdxBuilderHelper(IndexReader idxReader,
			IndexDirectory idxDirectory, String expressivity)
			throws EnvironmentLockedException, DatabaseException, IOException,
			StorageException {

		s_idxDirectory = idxDirectory;
		s_idxReader = idxReader;
		s_structureIndex = s_idxReader.getStructureIndex();

		s_expressivity = expressivity;

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

	public int getDataType(String prop) throws IOException, DatabaseException {

		if (isDataProperty(prop)) {

			String dataType = null;

			if ((dataType = FacetDbUtils.get(s_cacheDB, "dataType_" + prop,
					s_stringBinding)) == null) {

				try {

					Table<String> triples = s_idxReader
							.getDataIndex()
							.getTriples(
									prop,
									"http://www.w3.org/2000/01/rdf-schema#range",
									null);

					if (!triples.getRows().isEmpty()) {
						
						dataType = String.valueOf(FacetUtils
								.range2DataType(triples.getRow(0)[2]));
					} else {
						dataType = String.valueOf(DataType.NOT_SET);
					}
				} catch (StorageException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (NullPointerException e) {
					e.printStackTrace();
				}

				if (dataType != null) {
					FacetDbUtils.store(s_cacheDB, "dataType_" + prop, dataType,
							s_stringBinding);
				}
			}

			return Integer.parseInt(dataType);

		} else {
			return DataType.NOT_SET;
		}
	}

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

	public String getLabel(String res) throws DatabaseException, IOException {

		String label = null;

		if ((label = FacetDbUtils.get(s_cacheDB, "label_" + res,
				s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader.getDataIndex()
						.getTriples(res,
								"http://www.w3.org/2000/01/rdf-schema#label",
								null);

				if (!triples.getRows().isEmpty()) {
					label = FacetUtils.getValueOfLiteral(triples.getRow(0)[2]);
				} else {
					label = FacetEnvironment.DefaultValue.NO_LABEL;
				}

			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (label != null) {
				FacetDbUtils.store(s_cacheDB, "label_" + res, label,
						s_stringBinding);
			}
		}

		return label;
	}

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
					rangeClassLabel = "Range: "+triples.getRow(0)[2];
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
		int ftype = isDataProperty(property)
				? FacetType.DATAPROPERTY_BASED
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

			Node superPropertyNode = new Node();
			superPropertyNode.setValue(superProperty);
			superPropertyNode.setContent(isDataProperty(property)
					? NodeContent.DATA_PROPERTY
					: NodeContent.OBJECT_PROPERTY);

			
			Facet facet = new Facet(superProperty, ftype,
					DataType.NOT_SET);

			facet.setDataType(getDataType(property));
			facet.setLabel(getLabel(property));
			
			superPropertyNode.setFacet(facet);

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
				FacetEnvironment.DatabaseName.FH_CACHE, config);

		s_stringBinding = TupleBinding.getPrimitiveBinding(String.class);
	}

	private void initClasses(IndexReader idxReader) {

		try {

			s_classes = new HashSet<String>();

			Table<String> triples;

			if (s_expressivity.equals(Expressivity.RDF)) {

				triples = idxReader.getDataIndex().getTriples(null,
						"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
						"http://www.w3.org/2000/01/rdf-schema#Class");

			} else {

				triples = idxReader.getDataIndex().getTriples(null,
						"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
						"http://www.w3.org/2002/07/owl#Class");
			}

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

	public boolean isDataProperty(String prop) throws IOException,
			DatabaseException {

		String isDataProperty = null;

		if ((isDataProperty = FacetDbUtils.get(s_cacheDB, "isDataProperty_"
				+ prop, s_stringBinding)) == null) {

			try {

				Table<String> triples = s_idxReader
						.getDataIndex()
						.getTriples(
								prop,
								"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
								null);

				if (!triples.getRows().isEmpty()) {

					if (triples.getRow(0)[2]
							.equals("http://www.w3.org/2002/07/owl#DatatypeProperty")) {
						isDataProperty = "1";
					} else {
						isDataProperty = "0";
					}
				} else {
					isDataProperty = "1";
				}
			} catch (StorageException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}

			if (isDataProperty != null) {
				FacetDbUtils.store(s_cacheDB, "isDataProperty_" + prop,
						isDataProperty, s_stringBinding);
			}
		}

		return isDataProperty.equals("1");
	}

	public boolean isObjectProperty(String prop) throws IOException,
			DatabaseException {
		return !isDataProperty(prop);
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

				while ((superClass = getSuperClass(currentClass)) != null) {

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
}
