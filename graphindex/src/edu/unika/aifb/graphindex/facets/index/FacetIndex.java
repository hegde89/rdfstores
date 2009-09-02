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
package edu.unika.aifb.graphindex.facets.index;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import cern.colt.bitvector.BitVector;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.facets.FacetDbUtils;
import edu.unika.aifb.graphindex.facets.index.builder.impl.FacetIndexBuilderHelper;
import edu.unika.aifb.graphindex.facets.index.builder.impl.FacetTreeBuilder;
import edu.unika.aifb.graphindex.facets.index.builder.impl.FacetVPosIndexBuilder;
import edu.unika.aifb.graphindex.facets.model.impl.FacetTree;
import edu.unika.aifb.graphindex.facets.model.impl.Node;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.Util;

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

	public void build() throws EnvironmentLockedException, DatabaseException,
			IOException, StorageException {

		IndexReader idxReader = new IndexReader(this.m_idxDirectory);

		FacetIndexBuilderHelper helper = FacetIndexBuilderHelper.getInstance(
				idxReader, this.m_idxDirectory);

		// Vector Position Index
		FacetVPosIndexBuilder vPosIndexBuilder = new FacetVPosIndexBuilder(
				this.m_idxDirectory, idxReader, helper);
		vPosIndexBuilder.build();
		vPosIndexBuilder.close();

		helper.setVPosIndex(this.getVPosIndex());

		// Distance Index

		// Facet Tree Index

		FacetTreeBuilder treeBuilder = new FacetTreeBuilder(
				this.m_idxDirectory, this.m_idxConfig, idxReader, helper);
		treeBuilder.build();
		treeBuilder.close();

		FacetIndexBuilderHelper.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.unika.aifb.graphindex.index.Index#close()
	 */
	@Override
	public void close() throws StorageException, DatabaseException {

		if (this.m_treeDB != null) {
			this.m_treeDB.close();
		}

		if (this.m_leaveDB != null) {
			this.m_leaveDB.close();
		}

		if ((this.m_treeDB != null) || (this.m_leaveDB != null)) {
			this.m_env.close();
		}

		if (this.m_vPosIndex != null) {
			this.m_vPosIndex.close();
		}

		this.m_leaveDB = null;
		this.m_treeDB = null;
		this.m_env = null;
		this.m_vPosIndex = null;

		System.gc();
	}

	public FacetTree getFacetTree(String extension) throws DatabaseException,
			IOException {

		if (this.m_treeDB == null) {
			this.initDBs();
		}

		s_log.debug("looking for facet tree for extension: " + extension);

		FacetTree tree = null;

		DatabaseEntry dbKey = new DatabaseEntry(Util.intToBytes(extension
				.hashCode()));
		DatabaseEntry out = new DatabaseEntry();

		this.m_treeDB.get(null, dbKey, out, null);

		if (out.getData() != null) {

			Object object = Util.bytesToObject(out.getData());

			if (object instanceof FacetTree) {
				tree = (FacetTree) object;
			} else {
				s_log.error("key found, however, data was no tree.");
			}
		} else {
			s_log.debug("no tree found!");
		}

		return tree;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Node> getFacetTreeLeaves(String extension,
			String objectValue) throws DatabaseException, IOException {

		if (this.m_leaveDB == null) {
			this.initDBs();
		}

		s_log.debug("looking for leaves for extension / object: " + extension
				+ " / " + objectValue);

		ArrayList<Node> nodes = null;

		DatabaseEntry dbKey = new DatabaseEntry(Util
				.intToBytes((extension + objectValue).hashCode()));
		DatabaseEntry out = new DatabaseEntry();

		this.m_leaveDB.get(null, dbKey, out, null);

		if (out.getData() != null) {

			Object object = Util.bytesToObject(out.getData());

			if (object instanceof ArrayList) {
				nodes = (ArrayList<Node>) object;
			} else {
				s_log.error("key found, however, data was no arraylist.");
			}
		} else {
			s_log.debug("no data found!");
		}

		return nodes;
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

		int pos = -1;

		if (this.m_vPosIndex == null) {
			this.initIndices();
		}

		pos = Integer.getInteger(this.m_vPosIndex.getDataItem(
				IndexDescription.ESV, DataField.VECTOR_POS, new String[] {
						extension, subject }));

		return pos;
	}

	public BitVector getSourceVectorForNode(double nodeId) {

		// TODO

		return null;
	}

	/**
	 * @return the VPosIndex
	 * @throws IOException
	 */
	public LuceneIndexStorage getVPosIndex() throws IOException {

		if (this.m_vPosIndex == null) {
			this.initIndices();
		}

		return this.m_vPosIndex;
	}

	private void initDBs() throws EnvironmentLockedException,
			DatabaseException, IOException {

		s_log.debug("get db connection ...");

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(false);

		this.m_env = new Environment(this.m_idxDirectory.getDirectory(
				IndexDirectory.FACET_TREE_DIR, true), envConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(false);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);

		this.m_treeDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.TREE, config);
		this.m_leaveDB = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseName.LEAVE, config);

		s_log.debug("got db connection!");
	}

	private void initIndices() throws IOException {
		this.m_vPosIndex = new LuceneIndexStorage(this.m_idxDirectory
				.getDirectory(IndexDirectory.FACET_VPOS_DIR, false));

		this.m_vPosIndex.initialize(true, true);
	}
}
