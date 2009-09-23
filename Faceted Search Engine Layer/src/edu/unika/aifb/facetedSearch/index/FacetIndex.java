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
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.PreloadConfig;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetIndex extends Index {

	public enum FacetIndexName {
		TREE, LEAVE, OBJECT
		// SORTED_LITERALS,ENDPOINTS
	}

	private final static Logger s_log = Logger.getLogger(FacetIndex.class);

	private IndexDirectory m_idxDirectory;

	/*
	 * Indices
	 */

	private Environment m_env;
	private Environment m_env2;

	private Database m_treeDB;
	private Database m_leaveDB;
	private Database m_objectDB;
	private Database m_classDB;

	/*
	 * Bindings
	 */
	private SerialBinding<FacetTree> m_treeBinding;
	private EntryBinding<Double> m_leaveBinding;
	private EntryBinding<String> m_objBinding;

	public FacetIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig)
			throws EnvironmentLockedException, DatabaseException, IOException {

		super(idxDirectory, idxConfig);
		m_idxDirectory = idxDirectory;

		init();

	}

	@Override
	public void close() throws StorageException {

		if (m_treeDB != null) {

			try {
				m_treeDB.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_leaveDB != null) {

			try {
				m_leaveDB.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_objectDB != null) {

			try {
				m_objectDB.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_classDB != null) {

			try {
				m_classDB.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_env != null) {

			try {
				m_env.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		if (m_env2 != null) {

			try {
				m_env2.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		m_leaveDB = null;
		m_treeDB = null;
		m_objectDB = null;
		m_env = null;
		m_env2 = null;

		System.gc();
	}

	// public HashSet<String> getExtensions(Node node, String sourceInd)
	// throws EnvironmentLockedException, DatabaseException, IOException {
	//
	// if (m_objectDB == null) {
	// init();
	// }
	//
	// return FacetDbUtils.getAllAsSet(m_objectDB, sourceInd
	// + node.getPathHashValue() + "ext", m_objBinding);
	// }

	public Database getIndex(FacetIndexName idxName)
			throws EnvironmentLockedException, DatabaseException, IOException {

		switch (idxName) {

		case TREE: {

			if (m_treeDB == null) {
				init();
			}

			return m_treeDB;
		}
		case LEAVE: {

			if (m_leaveDB == null) {
				init();
			}

			return m_leaveDB;
		}
		case OBJECT: {

			if (m_objectDB == null) {
				init();
			}

			return m_objectDB;
		}
		default:
			return null;
		}
	}

	public HashSet<Double> getLeaves(String extension, String sourceIndividual)
			throws DatabaseException, IOException {

		if (m_leaveDB == null) {
			init();
		}

		return FacetDbUtils.getAllAsSet(m_leaveDB, FacetDbUtils
				.getKey(new String[] { extension, sourceIndividual }),
				m_leaveBinding);
	}

	// public LuceneIndexStorage getLuceneIndex(FacetIndexName idxName)
	// throws EnvironmentLockedException, DatabaseException, IOException {
	//
	// switch (idxName) {
	//
	// case OBJECT: {
	//
	// if (m_objectIndex == null) {
	// initIndices();
	// }
	//
	// return m_objectIndex;
	// }
	// case SORTED_LITERALS: {
	//
	// if (m_sortedLitIndex == null) {
	// initIndices();
	// }
	//
	// return m_sortedLitIndex;
	// }
	// default:
	// return null;
	// }
	// }

	public HashSet<String> getObjects(Node leave, String sourceInd)
			throws EnvironmentLockedException, DatabaseException, IOException {

		if (m_objectDB == null) {
			init();
		}

		return FacetDbUtils.getAllAsSet(m_objectDB, sourceInd
				+ leave.getPathHashValue(), m_objBinding);
	}

	public FacetTree getTree(String extension) throws DatabaseException,
			IOException {

		if (m_treeDB == null) {
			init();
		}

		return FacetDbUtils.get(m_treeDB, extension, m_treeBinding);
	}

	private void init() throws EnvironmentLockedException, DatabaseException,
			IOException {

		s_log.debug("get db connection ...");

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(false);

		m_env = new Environment(m_idxDirectory
				.getDirectory(IndexDirectory.FACET_TREE_DIR), envConfig);

		m_env2 = new Environment(m_idxDirectory
				.getDirectory(IndexDirectory.FACET_OBJECTS_DIR), envConfig);

		// Databases without duplicates
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(false);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setDeferredWrite(true);
		dbConfig.setReadOnly(true);

		// Databases with duplicates
		DatabaseConfig dbConfig2 = new DatabaseConfig();
		dbConfig2.setTransactional(false);
		dbConfig2.setAllowCreate(false);
		dbConfig2.setSortedDuplicates(true);
		dbConfig2.setDeferredWrite(true);
		dbConfig2.setReadOnly(true);

		// Databases without duplicates
		m_treeDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.TREE,
				dbConfig);

		// Databases with duplicates
		m_leaveDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.LEAVE,
				dbConfig2);

		m_objectDB = m_env2.openDatabase(null,
				FacetDbUtils.DatabaseNames.OBJECT, dbConfig2);

		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(FacetEnvironment.DefaultValue.PRELOAD_TIME);

		m_treeDB.preload(pc);
		m_leaveDB.preload(pc);
		m_objectDB.preload(pc);

		m_classDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.CLASS,
				dbConfig);

		// Create the bindings
		m_treeBinding = new SerialBinding<FacetTree>(new StoredClassCatalog(
				m_classDB), FacetTree.class);
		m_leaveBinding = TupleBinding.getPrimitiveBinding(Double.class);
		m_objBinding = TupleBinding.getPrimitiveBinding(String.class);

		s_log.debug("got db connection!");
	}

	// private void initIndices() throws IOException {
	//
	// m_objectIndex = new LuceneIndexStorage(m_idxDirectory.getDirectory(
	// IndexDirectory.FACET_OBJECTS_DIR, false), m_idxReader
	// .getCollector());
	//
	// m_objectIndex.initialize(true, true);
	//
	// m_sortedLitIndex = new LuceneIndexStorage(m_idxDirectory.getDirectory(
	// IndexDirectory.FACET_LITERALS_DIR, false), m_idxReader
	// .getCollector());
	//
	// m_sortedLitIndex.initialize(true, true);
	// }
}
