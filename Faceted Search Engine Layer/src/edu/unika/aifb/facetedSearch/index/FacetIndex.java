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
import java.util.HashSet;
import java.util.Iterator;
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
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;
import edu.unika.aifb.facetedSearch.facets.model.impl.Resource;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.db.binding.NodeBinding;
import edu.unika.aifb.facetedSearch.index.db.binding.PathBinding;
import edu.unika.aifb.facetedSearch.index.db.util.FacetDbUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

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
	private Environment m_env;

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
	 * Maps ...
	 */

	private StoredMap<String, Node> m_leaveMap;

	/*
	 * Bindings
	 */
	private EntryBinding<Queue<Edge>> m_pathBinding;
	private EntryBinding<Node> m_nodeBinding;
	private EntryBinding<String> m_strgBinding;

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

		Collection<Node> leaves = m_leaveMap.duplicates(fv.getValue());

		if ((leaves == null) || leaves.isEmpty()) {
			s_log.error("no leaves found for individual '" + fv.getValue()
					+ "'");
		}

		return leaves;
	}

	public Collection<Node> getLeaves(String srcInd) throws DatabaseException,
			IOException {

		if (m_leaveDB == null) {
			init();
		}

		Collection<Node> leaves = m_leaveMap.duplicates(srcInd);

		if ((leaves == null) || leaves.isEmpty()) {
			s_log.error("no leaves found for individual '" + srcInd + "'");
		}

		return leaves;
	}

	public Collection<AbstractSingleFacetValue> getObjects(Node leave,
			String subject) throws EnvironmentLockedException,
			DatabaseException, IOException {

		HashSet<AbstractSingleFacetValue> fvs = new HashSet<AbstractSingleFacetValue>();

		try {

			Table<String> objectsTriples = m_idxReader.getDataIndex()
					.getTriples(subject, leave.getFacet().getUri(), null);

			Iterator<String[]> objectsTriplesIter = objectsTriples.getRows()
					.iterator();

			while (objectsTriplesIter.hasNext()) {

				String object = objectsTriplesIter.next()[2];

				m_idxReader.getDataIndex().getTriples(subject,
						leave.getFacet().getUri(), null);

				AbstractSingleFacetValue fv;

				if (Util.isEntity(object)) {

					fv = new Resource();
					((Resource) fv).setValue(object);
					((Resource) fv).setRangeExt("");
					((Resource) fv).setSourceExt("");
					((Resource) fv).setIsResource(true);
					((Resource) fv)
							.setLabel(FacetEnvironment.DefaultValue.NO_LABEL);

				} else {

					fv = new Literal();
					((Literal) fv).setValue(object);
					((Literal) fv).setRangeExt("");
					((Literal) fv).setSourceExt("");
					((Literal) fv).setIsResource(false);
					((Literal) fv)
							.setLabel(FacetEnvironment.DefaultValue.NO_LABEL);
				}

				fvs.add(fv);
			}

		} catch (StorageException e) {

			e.printStackTrace();
		}

		return fvs;
	}

	public Queue<Edge> getPath2Root(String path) throws DatabaseException,
			IOException {

		Queue<Edge> path2Root = FacetDbUtils.get(m_pathDB, path, m_pathBinding);

		if ((path2Root == null) || path2Root.isEmpty()) {
			s_log.error("path to root is empty: " + path);
		}

		return path2Root;
	}

	private void init() throws EnvironmentLockedException, DatabaseException,
			IOException {

		s_log.debug("get db connection ...");

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(false);

		m_env = new Environment(FacetedSearchLayerConfig.getFacetTreeIdxDir(),
				envConfig);

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

		m_dbs = new ArrayList<Database>();
		m_dbs.add(m_pathDB);
		m_dbs.add(m_leaveDB);

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

		/*
		 * Create maps on top of dbs ...
		 */
		m_leaveMap = new StoredMap<String, Node>(m_leaveDB, m_strgBinding,
				m_nodeBinding, false);

		s_log.debug("got db connection!");
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
