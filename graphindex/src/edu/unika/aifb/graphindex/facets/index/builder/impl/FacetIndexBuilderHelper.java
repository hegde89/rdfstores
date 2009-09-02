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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

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
import edu.unika.aifb.graphindex.facets.model.impl.Node;
import edu.unika.aifb.graphindex.facets.model.impl.Node.NodeContent;
import edu.unika.aifb.graphindex.facets.model.impl.Node.NodeType;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.Util;

public class FacetIndexBuilderHelper {

	private static LuceneIndexStorage s_vPosIndex;
	private static StructureIndex s_structureIndex;
	private static IndexDirectory s_idxDirectory;
	private static IndexReader s_idxReader;
	private static Environment s_env;
	private static Database s_cacheDB;
	private static HashSet<String> s_classes;
	private static FacetIndexBuilderHelper s_instance;
	private static ArrayList<String> s_objectProperties;
	private static ArrayList<String> s_dataProperties;

	private static DirectedMultigraph<NodeElement, EdgeElement> s_indexGraph;

	public static void close() throws DatabaseException, StorageException {

		s_vPosIndex.close();
		s_structureIndex.close();
		s_cacheDB.close();

		s_env.removeDatabase(null, FacetDbUtils.DatabaseName.FH_CACHE);
		s_env.close();

		s_classes = null;
		s_objectProperties = null;
		s_dataProperties = null;
		s_instance = null;

		System.gc();
	}

	public static FacetIndexBuilderHelper getInstance(IndexReader idxReader,
			IndexDirectory idxDirectory) throws EnvironmentLockedException,
			DatabaseException, IOException {

		return s_instance == null ? s_instance = new FacetIndexBuilderHelper(
				idxReader, idxDirectory) : s_instance;
	}

	private FacetIndexBuilderHelper(IndexReader idxReader,
			IndexDirectory idxDirectory) throws EnvironmentLockedException,
			DatabaseException, IOException {

		s_idxDirectory = idxDirectory;
		s_idxReader = idxReader;
		s_structureIndex = s_idxReader.getStructureIndex();

		s_objectProperties = new ArrayList<String>();
		s_dataProperties = new ArrayList<String>();

		this.initGraphIndex(idxReader);
		this.initClasses(idxReader);
		this.initCache(idxDirectory);

	}

	private Object get(String key) throws DatabaseException {

		Object res = null;

		DatabaseEntry dbKey = new DatabaseEntry(Util.intToBytes(key.hashCode()));
		DatabaseEntry out = new DatabaseEntry();

		Cursor cursor = s_cacheDB.openCursor(null, null);

		if (cursor.getSearchKey(dbKey, out, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

			if (out.getData() != null) {
				res = Util.bytesToObject(out.getData());
			}
		}

		return res;
	}

	public String getClass(String individual) throws DatabaseException {

		String clazz = null;

		if ((clazz = (String) this.get("class_" + individual)) == null) {

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
				FacetDbUtils.store(s_cacheDB, "class_" + individual, clazz);
			}
		}

		return clazz;
	}

	public String getExtension(String object) throws StorageException,
			IOException, DatabaseException {

		String extension = null;

		if ((extension = (String) this.get("ex_" + object)) == null) {

			extension = s_structureIndex.getExtension(object);

			if (extension != null) {
				FacetDbUtils.store(s_cacheDB, "ex_" + object, extension);
			}
		}

		return extension;
	}

	public DirectedMultigraph<NodeElement, EdgeElement> getIndexGraph() {
		return s_indexGraph;
	}

	public int getPosition(String extension, String subject)
			throws IOException, StorageException {

		if (s_vPosIndex == null) {

			s_vPosIndex = new LuceneIndexStorage(s_idxDirectory.getDirectory(
					IndexDirectory.FACET_VPOS_DIR, false));

			s_vPosIndex.initialize(true, true);

		}

		String posString = s_vPosIndex.getDataItem(IndexDescription.ESV,
				DataField.VECTOR_POS, new String[] { extension, subject });

		return Integer.parseInt(posString);
	}

	public Node getRange(Node property) throws DatabaseException {

		String rangeClassLabel = null;

		if ((rangeClassLabel = (String) this
				.get("range_" + property.getValue())) == null) {

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
						rangeClassLabel);
			}
		}

		return rangeClassLabel == null ? null : new Node(rangeClassLabel,
				NodeType.RANGE_TOP, NodeContent.CLASS);
	}

	public Node getSuperClass(String clazz) throws DatabaseException {

		String superClass = null;

		if ((superClass = (String) this.get("superClass_" + clazz)) == null) {

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
				FacetDbUtils
						.store(s_cacheDB, "superClass_" + clazz, superClass);
			}
		}

		return superClass == null ? null : new Node(superClass,
				NodeType.INNER_NODE, NodeContent.CLASS);
	}

	public Node getSuperProperty(String property) throws IOException,
			DatabaseException {

		String superProperty = null;

		if ((superProperty = (String) this.get("superProperty_" + property)) == null) {

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
						superProperty);
			}
		}

		return superProperty == null ? null : new Node(superProperty, this
				.isDataProperty(property) ? NodeContent.DATA_PROPERTY
				: NodeContent.OBJECT_PROPERTY);
	}

	public boolean hasRangeClass(Node property) throws DatabaseException {

		return this.getRange(property) == null ? false : true;
	}

	private void initCache(IndexDirectory idxDirectory)
			throws EnvironmentLockedException, DatabaseException, IOException {

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		s_env = new Environment(idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true), envConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);

		s_cacheDB = s_env.openDatabase(null,
				FacetDbUtils.DatabaseName.FH_CACHE, config);

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
	 */
	public boolean isSubClassOf(Node class1, Node class2)
			throws DatabaseException {

		String isSubClass = null;

		if ((isSubClass = (String) this.get("subClassOf_" + class1 + "_"
				+ class2)) == null) {

			isSubClass = "0";

			if (class1.equals(class2)) {
				isSubClass = "0";
			} else {

				String currentClass = class1.getValue();
				Node superClass;

				while ((superClass = this.getSuperClass(currentClass)) != null) {

					if (superClass.equals(class2)) {

						isSubClass = "1";
						break;
					} else {
						currentClass = superClass.getValue();
					}
				}
			}

			if (isSubClass != null) {
				FacetDbUtils.store(s_cacheDB, "subClassOf_" + class1 + "_"
						+ class2, isSubClass);
			}
		}

		return isSubClass.equals("0") ? false : true;
	}

	public void setVPosIndex(LuceneIndexStorage vPosIndex) {

		s_vPosIndex = vPosIndex;
	}

}
