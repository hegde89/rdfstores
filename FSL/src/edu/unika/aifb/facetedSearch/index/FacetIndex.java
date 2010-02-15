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
package edu.unika.aifb.facetedSearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.RDF;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.PreloadConfig;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.db.binding.NodeBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.PathBinding;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetIndex extends Index {

	public enum FacetIndexName {
		PATH, LEAVE, OBJECT
	}

	private final static Logger s_log = Logger.getLogger(FacetIndex.class);

	/*
	 * 
	 */
	private Environment m_envIdx;
	private Environment m_envCache;

	/*
	 * 
	 */
	private ArrayList<Database> m_dbs;

	/*
	 * Indices
	 */
	private Database m_pathDB;
	private Database m_leaveDB;

	/*
	 * 
	 */
	private Database m_sharedCache;
	private Database m_classDB;

	/*
	 * Maps ...
	 */
	private StoredMap<String, AbstractSingleFacetValue> m_sharedCacheMap;
	private StoredMap<String, Node> m_leaveMap;
	private StoredMap<String, Queue<Edge>> m_pathMap;

	/*
	 * Bindings
	 */
	private EntryBinding<Queue<Edge>> m_pathBinding;
	private EntryBinding<Node> m_nodeBinding;
	private EntryBinding<String> m_strgBinding;
	private EntryBinding<AbstractSingleFacetValue> m_abstractSingleFacetValueBinding;

	/*
	 * 
	 */
	private IndexReader m_idxReader;

	public FacetIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig)
			throws EnvironmentLockedException, DatabaseException, IOException {

		super(idxDirectory, idxConfig);
		m_idxReader = new IndexReader(idxDirectory);

		init();
	}

	@Override
	public void close() {

		m_sharedCacheMap.clear();

		for (Database db : m_dbs) {
			try {
				db.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_envIdx != null) {

			try {
				m_envIdx.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_envCache != null) {

			try {
				m_envCache.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}

	public Database getIndex(FacetIndexName idxName)
			throws EnvironmentLockedException, DatabaseException, IOException {

		switch (idxName) {

			case PATH : {

				if (m_pathDB == null) {
					init();
				}

				return m_pathDB;
			}
			case LEAVE : {

				if (m_leaveDB == null) {
					init();
				}

				return m_leaveDB;
			}
			default : {
				return null;
			}
		}
	}

	public Collection<Node> getLeaves(AbstractSingleFacetValue fv)
			throws DatabaseException, IOException {

		if (m_leaveDB == null) {
			init();
		}

		Collection<Node> leaves = m_leaveMap.duplicates(FacetUtils
				.encodeLocalName(fv.getValue()));

		 if ((leaves == null) || leaves.isEmpty()) {
			 
//			s_log.error("no leaves found for individual '" + fv.getValue()
//					+ "'");
			
			leaves = m_leaveMap.duplicates(fv.getValue());
		}

		return leaves;
	}

	public Collection<Node> getLeaves(String srcInd) throws DatabaseException,
			IOException {

		if (m_leaveDB == null) {
			init();
		}

		Collection<Node> leaves = m_leaveMap.duplicates(FacetUtils
				.encodeLocalName(srcInd));

		// if ((leaves == null) || leaves.isEmpty()) {
		//
		// // s_log.error("no leaves found for individual '" + srcInd + "'");
		// // TODO
		//
		// if ((leaves == null) || leaves.isEmpty()) {
		//
		// try {
		//
		// Table<String> resTriples = m_idxReader.getDataIndex()
		// .getTriples(FacetUtils.encodeLocalName(srcInd),
		// null, null);
		//
		// Iterator<String[]> resTripleIter = resTriples.getRows()
		// .iterator();
		//
		// if (resTripleIter.hasNext()) {
		//
		// while (resTripleIter.hasNext()) {
		//
		// String[] resTiple = resTripleIter.next();
		//
		// if (!FacetEnvironment.PROPERTIES_TO_IGNORE
		// .contains(resTiple[1])
		// && !resTiple[1]
		// .startsWith(FacetEnvironment.OWL.NAMESPACE)) {
		//
		// System.out.println("ERROR: resTiple: "
		// + resTiple[0] + ", " + resTiple[1]
		// + ", " + resTiple[2]);
		// }
		// }
		// } else {
		//
		// System.out.println("No triples found for: "
		// + (new URL(srcInd)).toString());
		// String ext = m_idxReader.getStructureIndex()
		// .getExtension((new URL(srcInd)).toString());
		// System.out.println("ext: " + ext);
		// }
		//
		// } catch (StorageException e) {
		// e.printStackTrace();
		// }
		// }
		// }

		return leaves;
	}

	public Collection<AbstractSingleFacetValue> getObjects(Node leave,
			String subject) throws EnvironmentLockedException,
			DatabaseException, IOException {

		String key = leave.getCurrentFacet().getUri() + leave.getValue()
				+ subject;

		if (m_sharedCacheMap.duplicates(key).isEmpty()) {

			try {

				Table<String> objectsTriples = m_idxReader.getDataIndex()
						.getTriples(FacetUtils.encodeLocalName(subject),
								leave.getCurrentFacet().getUri(), null);

				if (objectsTriples.rowCount() == 0) {
					objectsTriples = m_idxReader.getDataIndex().getTriples(
							subject, leave.getCurrentFacet().getUri(), null);
				}

				Iterator<String[]> objectsTriplesIter = objectsTriples
						.getRows().iterator();

				if (leave.getCurrentFacet().isObjectPropertyBased()) {

					boolean foundType = false;

					while (objectsTriplesIter.hasNext()) {

						foundType = false;

						String object = FacetUtils.cleanURI(objectsTriplesIter
								.next()[2]);

						Table<String> typeTriples = m_idxReader.getDataIndex()
								.getTriples(object, RDF.TYPE.stringValue(),
										null);

						Iterator<String[]> typeIter = typeTriples.getRows()
								.iterator();

						if (typeIter.hasNext()) {

							while (typeIter.hasNext()) {

								String type = FacetUtils.cleanURI(typeIter
										.next()[2]);

								if (type.equals(leave.getValue())) {
									foundType = true;
									break;
								}
							}

							if (foundType) {

								AbstractSingleFacetValue fv = new Resource();
								((Resource) fv).setValue(object);
								((Resource) fv).setIsResource(true);
								((Resource) fv)
										.setLabel(FacetEnvironment.DefaultValue.NO_LABEL);

								m_sharedCacheMap.put(key, fv);
							}
						} else {

							AbstractSingleFacetValue fv = new Resource();
							((Resource) fv).setValue(object);
							((Resource) fv).setIsResource(true);
							((Resource) fv)
									.setLabel(FacetEnvironment.DefaultValue.NO_LABEL);

							m_sharedCacheMap.put(key, fv);
						}
					}
				} else {

					boolean valid = true;
					int dataType = leave.getCurrentFacet().getDataType();

					while (objectsTriplesIter.hasNext()) {

						String lit = objectsTriplesIter.next()[2];
						String litValue = FacetUtils.getLiteralValue(lit);

						AbstractSingleFacetValue fv = new Literal();
						((Literal) fv).setValue(lit);
						((Literal) fv).setIsResource(false);
						((Literal) fv)
								.setLabel(FacetEnvironment.DefaultValue.NO_LABEL);
						((Literal) fv).setLiteralValue(litValue);

						if (dataType == FacetEnvironment.DataType.DATE) {

							try {

								((Literal) fv).setParsedLiteral(XMLDatatypeUtil
										.parseCalendar(litValue));

							} catch (IllegalArgumentException e) {
								valid = false;
								s_log.debug("literal '" + fv + "' not valid!");
							}

						} else if (dataType == FacetEnvironment.DataType.NUMERICAL) {

							try {

								((Literal) fv).setParsedLiteral(XMLDatatypeUtil
										.parseDouble(litValue));

							} catch (IllegalArgumentException e) {
								valid = false;
								s_log.debug("literal '" + fv + "' not valid!");
							}
						} else {

							((Literal) fv).setParsedLiteral(lit);
						}

						if (valid) {
							m_sharedCacheMap.put(key, fv);
						}
					}
				}
			} catch (StorageException e) {
				e.printStackTrace();
			}
		}

		return m_sharedCacheMap.duplicates(key);
	}

	public Queue<Edge> getPath2Root(String path) throws DatabaseException,
			IOException {

		Queue<Edge> path2Root = m_pathMap.get(path);

		// if ((path2Root == null) || path2Root.isEmpty()) {
		// s_log.error("path to root is empty: " + path);
		// }

		return path2Root;
	}

	private void init() throws EnvironmentLockedException, DatabaseException,
			IOException {

		/*
		 * no creation allowed
		 */

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(false);

		/*
		 * creation allowed
		 */
		EnvironmentConfig envConfig2 = new EnvironmentConfig();
		envConfig2.setTransactional(false);
		envConfig2.setAllowCreate(true);

		m_envIdx = new Environment(FacetedSearchLayerConfig
				.getFacetTreeIdxDir(), envConfig);

		m_envCache = new Environment(FacetedSearchLayerConfig
				.getSharedCacheDir(), envConfig2);

		/*
		 * Databases without duplicates, read-only
		 */
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(false);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setDeferredWrite(true);
		dbConfig.setReadOnly(true);

		/*
		 * Databases with duplicates, read-only
		 */
		DatabaseConfig dbConfig2 = new DatabaseConfig();
		dbConfig2.setTransactional(false);
		dbConfig2.setAllowCreate(false);
		dbConfig2.setSortedDuplicates(true);
		dbConfig2.setDeferredWrite(true);
		dbConfig2.setReadOnly(true);

		/*
		 * Databases without duplicates, read & write, allow create
		 */
		DatabaseConfig dbConfig3 = new DatabaseConfig();
		dbConfig3.setTransactional(false);
		dbConfig3.setAllowCreate(true);
		dbConfig3.setSortedDuplicates(false);
		dbConfig3.setDeferredWrite(true);

		/*
		 * Databases with duplicates, read & write, allow create
		 */
		DatabaseConfig dbConfig4 = new DatabaseConfig();
		dbConfig4.setTransactional(false);
		dbConfig4.setAllowCreate(true);
		dbConfig4.setSortedDuplicates(false);
		dbConfig4.setDeferredWrite(true);

		/*
		 * Create the bindings
		 */
		m_pathBinding = new PathBinding();
		m_nodeBinding = new NodeBinding();
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);

		m_classDB = m_envCache.openDatabase(null,
				FacetEnvironment.DatabaseName.CLASS, dbConfig3);

		try {

			StoredClassCatalog cata = new StoredClassCatalog(m_classDB);
			m_abstractSingleFacetValueBinding = new SerialBinding<AbstractSingleFacetValue>(
					cata, AbstractSingleFacetValue.class);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		/*
		 * 
		 */
		s_log.debug("create shared cache ...");

		m_sharedCache = m_envCache.openDatabase(null,
				FacetEnvironment.DatabaseName.SHARED_CACHE, dbConfig4);

		m_sharedCacheMap = new StoredMap<String, AbstractSingleFacetValue>(
				m_sharedCache, m_strgBinding,
				m_abstractSingleFacetValueBinding, true);

		s_log.debug("shared cache ready!");

		/*
		 * 
		 */
		s_log.debug("get db connection ...");

		m_pathDB = m_envIdx.openDatabase(null,
				FacetEnvironment.DatabaseName.PATH, dbConfig);
		m_leaveDB = m_envIdx.openDatabase(null,
				FacetEnvironment.DatabaseName.LEAVE, dbConfig2);

		/*
		 * Create maps on top of dbs ...
		 */
		m_leaveMap = new StoredMap<String, Node>(m_leaveDB, m_strgBinding,
				m_nodeBinding, false);

		m_pathMap = new StoredMap<String, Queue<Edge>>(m_pathDB, m_strgBinding,
				m_pathBinding, false);

		s_log.debug("got db connection!");

		/*
		 * 
		 */
		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_pathDB);
		m_dbs.add(m_leaveDB);
		m_dbs.add(m_sharedCache);

		PreloadConfig pc = new PreloadConfig();
		pc.setMaxBytes(FacetedSearchLayerConfig.getPreloadMaxBytes());

		for (Database db : m_dbs) {
			db.preload(pc);
		}
	}

	public boolean isOpen() {

		boolean isOpen = true;

		for (Database db : m_dbs) {

			if (db == null) {
				isOpen = false;
				break;
			}
		}

		return isOpen;
	}
}