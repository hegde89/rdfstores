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

import cern.colt.bitvector.BitVector;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetTree;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.DbConfigFactory;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.EnvironmentFactory;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;

/**
 * @author andi
 * 
 */
public class FacetIndex extends Index {

	private final static Logger s_log = Logger.getLogger(FacetIndex.class);

	private IndexDirectory m_idxDirectory;

	private LuceneIndexStorage m_vPosIndex;

	private Environment m_env;
	private Database m_treeDB;
	private Database m_leaveDB;
	private Database m_literalDB;

	// private Database m_propEndPointDB;

	/**
	 * @param idxDirectory
	 * @param idxConfig
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws EnvironmentLockedException
	 */
	public FacetIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig)
			throws EnvironmentLockedException, DatabaseException, IOException {

		super(idxDirectory, idxConfig);

		this.m_idxDirectory = idxDirectory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.graphindex.index.Index#close()
	 */
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

		if (m_vPosIndex != null) {
			m_vPosIndex.close();
		}

		if (m_env != null) {
			try {
				this.m_env.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		m_leaveDB = null;
		m_treeDB = null;
		m_env = null;
		m_vPosIndex = null;

		System.gc();
	}

	// /**
	// * @return the endpointDB
	// * @throws IOException
	// * @throws DatabaseException
	// * @throws EnvironmentLockedException
	// */
	// public Database getEndPointDB() throws EnvironmentLockedException,
	// DatabaseException, IOException {
	//
	// if (m_propEndPointDB == null) {
	// initDBs();
	// }
	//
	// return m_propEndPointDB;
	// }

	public FacetTree getFacetTree(String extension) throws DatabaseException,
			IOException {

		if (m_treeDB == null) {
			initDBs();
		}

		return FacetDbUtils.get(m_treeDB, FacetDbUtils
				.getKey(new String[] { extension }));
	}

	public HashSet<Node> getLeaves(String extension, String sourceIndividual)
			throws DatabaseException, IOException {

		if (m_leaveDB == null) {
			initDBs();
		}

		return FacetDbUtils.get(m_leaveDB, FacetDbUtils.getKey(new String[] {
				extension, sourceIndividual }));
	}

	/**
	 * @return the leaveDB
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws EnvironmentLockedException
	 */
	public Database getLeaveDB() throws EnvironmentLockedException,
			DatabaseException, IOException {

		if (m_leaveDB == null) {
			initDBs();
		}

		return m_leaveDB;
	}

	/**
	 * @return the literalDB
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws EnvironmentLockedException
	 */
	public Database getLiteralDB() throws EnvironmentLockedException,
			DatabaseException, IOException {

		if (m_literalDB == null) {
			initDBs();
		}

		return m_literalDB;
	}

	/**
	 * 
	 * @param extension
	 * @param subject
	 * @return Position of subject in binary vector for this extension. Returns
	 *         '-1' if no record is found for this given key.
	 * @throws IOException
	 * @throws StorageException
	 */

	public int getPosition(String extension, String subject)
			throws IOException, StorageException {

		if (m_vPosIndex == null) {
			initIndices();
		}

		String posString = m_vPosIndex.getDataItem(IndexDescription.ESV,
				DataField.VECTOR_POS, new String[] { extension, subject });

		return posString == null ? -1 : Integer.parseInt(posString);
	}

	public BitVector getSourceVectorForNode(double nodeId) {

		// TODO

		return null;
	}

	/**
	 * @return the treeDB
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws EnvironmentLockedException
	 */
	public Database getTreeDB() throws EnvironmentLockedException,
			DatabaseException, IOException {

		if (m_treeDB == null) {
			initDBs();
		}

		return m_treeDB;
	}

	/**
	 * @return the VPosIndex
	 * @throws IOException
	 */
	public LuceneIndexStorage getVPosIndex() throws IOException {

		if (m_vPosIndex == null) {
			initIndices();
		}

		return m_vPosIndex;
	}

	private void initDBs() throws EnvironmentLockedException,
			DatabaseException, IOException {

		s_log.debug("get db connection ...");

		m_env = EnvironmentFactory.make(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true));

		DatabaseConfig config = DbConfigFactory.make(false);

		m_treeDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.TREE,
				config);

		// m_propEndPointDB = m_env.openDatabase(null,
		// FacetDbUtils.DatabaseNames.ENDPOINT, config);

		m_literalDB = m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.LITERAL, config);

		m_leaveDB = m_env.openDatabase(null, FacetDbUtils.DatabaseNames.LEAVE,
				DbConfigFactory.make(true));

		s_log.debug("got db connection!");
	}

	private void initIndices() throws IOException {

		m_vPosIndex = new LuceneIndexStorage(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_VPOS_DIR, false),
				new StatisticsCollector());

		m_vPosIndex.initialize(true, true);
	}
}
