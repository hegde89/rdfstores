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
import java.util.Queue;

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
import com.sleepycat.je.PreloadConfig;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.db.binding.AbstractSingleFacetValueBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.NodeBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.PathBinding;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;

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
	private IndexDirectory m_idxDirectory;

	/*
	 * 
	 */
	private Environment m_env;
	private Environment m_env2;

	/*
	 * 
	 */
	private ArrayList<Database> m_dbs;

	/*
	 * Indices
	 */
	private Database m_pathDB;
	private Database m_leaveDB;
	private Database m_objectDB;

	/*
	 * Maps ...
	 */
	private StoredMap<String, AbstractSingleFacetValue> m_objectMap;
	private StoredMap<String, Node> m_leaveMap;

	/*
	 * Bindings
	 */
	private EntryBinding<Queue<Edge>> m_pathBinding;
	private EntryBinding<Node> m_nodeBinding;
	private EntryBinding<AbstractSingleFacetValue> m_fvBinding;
	private EntryBinding<String> m_strgBinding;

	public FacetIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig)
			throws EnvironmentLockedException, DatabaseException, IOException {

		super(idxDirectory, idxConfig);
		m_idxDirectory = idxDirectory;

		init();

	}

	@Override
	public void close() {

		for (Database db : m_dbs) {
			try {
				db.close();
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
			case OBJECT : {

				if (m_objectDB == null) {
					init();
				}

				return m_objectDB;
			}
			default :
				return null;
		}
	}

	public Collection<Node> getLeaves(AbstractSingleFacetValue fv)
			throws DatabaseException, IOException {

		if (m_leaveDB == null) {
			init();
		}

		return m_leaveMap.duplicates(fv.getSourceExt() + fv.getValue());
	}

	public Collection<Node> getLeaves(String extension, String srcInd)
			throws DatabaseException, IOException {

		if (m_leaveDB == null) {
			init();
		}

		return m_leaveMap.duplicates(extension + srcInd);
	}

	public Collection<AbstractSingleFacetValue> getObjects(Node leave,
			String sourceInd) throws EnvironmentLockedException,
			DatabaseException, IOException {

		if (m_objectDB == null) {
			init();
		}

		return m_objectMap.duplicates(sourceInd + leave.getPathHashValue());
	}

	public Queue<Edge> getPath2RangeRoot(int pathHashValue)
			throws DatabaseException, IOException {

		return FacetDbUtils.get(m_pathDB, FacetEnvironment.Keys.RANGEROOT_PATH
				+ pathHashValue, m_pathBinding);
	}

	public Queue<Edge> getPath2Root(int pathHashValue)
			throws DatabaseException, IOException {

		return FacetDbUtils.get(m_pathDB, FacetEnvironment.Keys.ROOT_PATH
				+ pathHashValue, m_pathBinding);
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

		/*
		 * Databases without duplicates
		 */
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(false);
		dbConfig.setSortedDuplicates(false);
		dbConfig.setDeferredWrite(true);
		dbConfig.setReadOnly(true);

		m_pathDB = m_env.openDatabase(null, FacetEnvironment.DatabaseName.PATH,
				dbConfig);

		/*
		 * Databases with duplicates
		 */
		DatabaseConfig dbConfig2 = new DatabaseConfig();
		dbConfig2.setTransactional(false);
		dbConfig2.setAllowCreate(false);
		dbConfig2.setSortedDuplicates(true);
		dbConfig2.setDeferredWrite(true);
		dbConfig2.setReadOnly(true);

		m_leaveDB = m_env.openDatabase(null,
				FacetEnvironment.DatabaseName.LEAVE, dbConfig2);

		m_objectDB = m_env2.openDatabase(null,
				FacetEnvironment.DatabaseName.OBJECT, dbConfig2);

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_pathDB);
		m_dbs.add(m_leaveDB);
		m_dbs.add(m_objectDB);

		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(FacetEnvironment.DefaultValue.PRELOAD_TIME);

		for (Database db : m_dbs) {
			db.preload(pc);
		}

		/*
		 * Create the bindings
		 */
		m_pathBinding = new PathBinding();
		m_nodeBinding = new NodeBinding();
		m_strgBinding = TupleBinding.getPrimitiveBinding(String.class);
		m_fvBinding = new AbstractSingleFacetValueBinding();

		/*
		 * Create maps on top of dbs ...
		 */
		m_objectMap = new StoredMap<String, AbstractSingleFacetValue>(
				m_objectDB, m_strgBinding, m_fvBinding, false);

		m_leaveMap = new StoredMap<String, Node>(m_objectDB, m_strgBinding,
				m_nodeBinding, false);

		s_log.debug("got db connection!");
	}
}
