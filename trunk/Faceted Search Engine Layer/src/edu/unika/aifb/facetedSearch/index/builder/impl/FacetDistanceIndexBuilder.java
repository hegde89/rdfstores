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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Util;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.api.model.impl.Literal;
import edu.unika.aifb.facetedSearch.index.algo.distance.IDistanceMetric;
import edu.unika.aifb.facetedSearch.index.algo.distance.impl.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.index.distance.model.impl.LiteralComparator;
import edu.unika.aifb.facetedSearch.index.distance.model.impl.LiteralList;
import edu.unika.aifb.facetedSearch.index.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetUtil;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;

/**
 * @author andi
 * 
 */
public class FacetDistanceIndexBuilder implements IFacetIndexBuilder {

	private DirectedMultigraph<NodeElement, EdgeElement> m_idxGraph;
	private IndexDirectory m_idxDirectory;
	private FacetIndexBuilderHelper m_indexHelper;
	private LuceneIndexStorage m_distanceIndex;
	private IndexReader m_reader;

	private Database m_cache;
	private Environment m_env;

	private static final String s_cacheKey = "lits";
	private static final Logger s_log = Logger
			.getLogger(FacetDistanceIndexBuilder.class);

	
	public FacetDistanceIndexBuilder(IndexDirectory idxDirectory,
			IndexReader reader, FacetIndexBuilderHelper helper) {

		m_reader = reader;
		m_idxDirectory = idxDirectory;
		m_indexHelper = helper;

	}

	public void build() throws IOException, DatabaseException, StorageException {

		// init stuff
		m_distanceIndex = new LuceneIndexStorage(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_DISTANCES_DIR, true));

		m_distanceIndex.initialize(true, false);

		m_idxGraph = this.m_indexHelper.getIndexGraph();

		initDB();

		// build distance matrix
		buildDistanceIndex();
		// sort lists
		sortLiteralLists();

	}

	private void buildDistanceIndex() throws StorageException, IOException,
			DatabaseException {

		IndexStorage spIdx = m_reader.getStructureIndex().getSPIndexStorage();
		Set<NodeElement> extensions = m_idxGraph.vertexSet();

		// collect all literals and cache in db
		for (NodeElement extension : extensions) {

			List<String> entities = spIdx.getDataList(IndexDescription.EXTENT,
					DataField.ENT, extension.getLabel());

			for (String entity : entities) {

				if (Util.isLiteral(entity)) {

					ILiteral lit = new Literal(FacetUtil
							.getLiteralValue(entity));
					lit.setDataType(FacetUtil.getLiteralDataType(entity));
					lit.setExtension(extension.getLabel());

					FacetDbUtils.store(m_cache, s_cacheKey, lit);

				}
			}
		}

		// iterate over stored literals

		Stack<ILiteral> literals = FacetDbUtils.getAll(m_cache, s_cacheKey);

		while (!literals.isEmpty()) {

			ILiteral lit1 = literals.pop();
			DataType type1 = lit1.getDataType();

			for (ILiteral lit2 : literals) {

				DataType type2 = lit2.getDataType();
				BigDecimal distance = null;

				if ((type1 == type2)) {

					if ((type1 != DataType.UNKNOWN)
							&& (type2 != DataType.UNKNOWN)) {

						IDistanceMetric metric = DistanceMetricPool
								.getMetric(type1);

						distance = metric.getDistance(lit1, lit2);

					} else {

						s_log.debug("literal1 has type 'unknown' (" + type1
								+ ") or literal2 has type 'unkown' (" + type2
								+ ")");
					}
				} else {

					s_log.debug("literal1 has type '" + type1
							+ "' whereas literal2 has type '" + type2 + "'");
				}

				this.m_distanceIndex.addDataCompressed(IndexDescription.ELELD,
						new String[] { lit1.getExtension(), lit1.getValue(),
								lit2.getExtension(), lit2.getValue() }, String
								.valueOf(distance));
			}
		}
	}

	public void close() throws StorageException, DatabaseException {

		// close db
		if (m_cache != null) {
			m_cache.close();
		}

		// delete cache and close environment
		if (m_env != null) {
			m_env.removeDatabase(null, FacetDbUtils.DatabaseNames.FDB_CACHE);
			m_env.close();
		}

		// close lucene
		if (m_distanceIndex != null) {
			m_distanceIndex.optimize();
			m_distanceIndex.close();
		}
	}

	private void initDB() throws EnvironmentLockedException, DatabaseException,
			IOException {

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);

		m_env = new Environment(this.m_idxDirectory.getDirectory(
				IndexDirectory.FACET_DISTANCES_DIR, true), envConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(true);
		config.setDeferredWrite(true);

		m_cache = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FDB_CACHE, config);

	}

	private void sortLiteralLists() throws DatabaseException, IOException {

		Set<NodeElement> extensions = m_idxGraph.vertexSet();

		for (NodeElement extension : extensions) {

			s_log.debug("start sorting literals for extension '" + extension
					+ "'");

			HashMap<Node, HashSet<String>> propEndpoints = m_indexHelper
					.getPropEndPoints(extension.getLabel());

			for (Node prop : propEndpoints.keySet()) {

				if (prop.getContent() == NodeContent.DATA_PROPERTY) {

					LiteralList objects = m_indexHelper.getLiterals(extension
							.getLabel(), prop.getValue());

					// sort list
					Collections.sort(objects.getLiterals(),
							new LiteralComparator());

					// System.out.println("Sorted list:");
					// System.out.println("---------------------------");
					//
					// for (ILiteral object : objects) {
					// System.out.println("object: " + object.getValue());
					// System.out.println();
					// }
					//
					// System.out.println("---------------------------");

					// store list
					m_indexHelper.updateLiterals(objects, extension.getLabel(),
							prop.getValue());
				}
			}

			s_log.debug("finished sorting literals for extension: " + extension
					+ "!");
		}
	}
}
