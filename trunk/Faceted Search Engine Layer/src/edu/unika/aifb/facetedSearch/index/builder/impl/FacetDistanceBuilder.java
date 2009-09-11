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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment.DataType;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;
import edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.DistanceMetricPool;
import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node.NodeContent;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.index.model.impl.LiteralComparator;
import edu.unika.aifb.facetedSearch.index.model.impl.LiteralList;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.DbConfigFactory;
import edu.unika.aifb.facetedSearch.util.FacetDbUtils.EnvironmentFactory;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;

/**
 * @author andi
 * 
 */
public class FacetDistanceBuilder implements IFacetIndexBuilder {

	private DirectedMultigraph<NodeElement, EdgeElement> m_idxGraph;
	private IndexDirectory m_idxDirectory;
	private IndexReader m_idxReader;
	private FacetIndexHelper m_indexHelper;
	private LuceneIndexStorage m_distanceIndex;
	private boolean m_computeDistances;

	private Database m_cache;
	private Environment m_env;

	private static final Logger s_log = Logger
			.getLogger(FacetDistanceBuilder.class);

	private static final String DUMMY_ENTRY = "dummy";

	public FacetDistanceBuilder(IndexDirectory idxDirectory,
			FacetIndexHelper helper, IndexReader idxReader,
			boolean computeDistances) {

		m_computeDistances = computeDistances;
		m_idxDirectory = idxDirectory;
		m_idxReader = idxReader;
		m_indexHelper = helper;

	}

	public void build() throws IOException, DatabaseException, StorageException {

		if (m_computeDistances) {

			m_distanceIndex = new LuceneIndexStorage(m_idxDirectory
					.getDirectory(IndexDirectory.FACET_DISTANCES_DIR, true),
					m_idxReader.getCollector());

			m_distanceIndex.initialize(true, false);

			m_idxGraph = this.m_indexHelper.getIndexGraph();

			initDB();
			buildDistanceIndex();
		}

		// sort lists
		sortLiteralLists();
	}

	private void buildDistanceIndex() throws StorageException, IOException,
			DatabaseException {

		Set<NodeElement> extensions = m_idxGraph.vertexSet();

		boolean containsDistance;
		int count = 0;

		for (NodeElement extension : extensions) {

			s_log
					.debug("start computing distances for literals from extension '"
							+ extension
							+ "' ("
							+ (++count)
							+ " / "
							+ extensions.size() + ")");

			HashMap<Node, HashSet<String>> propEndpoints = m_indexHelper
					.getPropEndPoints(extension.getLabel());

			for (Node prop : propEndpoints.keySet()) {

				if (prop.getContent() == NodeContent.DATA_PROPERTY) {

					s_log.debug("start collecting literals for data-property '"
							+ prop.getValue() + "'");

					// List<String> rangeExtensions = prop.getRangeExtensions();
					//
					// // collect all literals and cache in db
					// for (String rangeExtension : rangeExtensions) {
					//
					// List<String> literals = spIdx.getDataList(
					// IndexDescription.EXTENT, DataField.ENT,
					// rangeExtension);
					//
					// for (String literalString : literals) {
					//
					// ILiteral lit = new Literal(FacetUtil
					// .getLiteralValue(literalString));
					// lit.setDataType(FacetUtil
					// .getLiteralDataType(literalString));
					// lit.setExtension(extension.getLabel());
					//
					// FacetDbUtils.store(m_cache, String.valueOf(prop
					// .getID()), lit);
					// }
					// }
					//
					// // iterate over stored literals
					// Cursor cursor1 = m_cache.openCursor(null, null);
					// ILiteral lit1;

					Stack<ILiteral> objects = new Stack<ILiteral>();
					objects.addAll(m_indexHelper.getLiterals(
							extension.getLabel(), prop.getValue())
							.getLiterals());

					s_log.debug("computing distances for " + objects.size()
							+ " literals ...");

					while (!objects.isEmpty()) {

						ILiteral lit1 = objects.pop();
						DataType type1 = lit1.getDataType();

						for (ILiteral lit2 : objects) {

							// String entry1 = m_distanceIndex.getDataItem(
							// IndexDescription.ELELD, DataField.DIS,
							// new String[] { lit1.getExtension(),
							// lit1.getValue(),
							// lit2.getExtension(),
							// lit2.getValue() });
							//
							// String entry2 = m_distanceIndex.getDataItem(
							// IndexDescription.ELELD, DataField.DIS,
							// new String[] { lit2.getExtension(),
							// lit2.getValue(),
							// lit1.getExtension(),
							// lit1.getValue() });

							containsDistance = FacetDbUtils.contains(m_cache,
									FacetDbUtils.getKey(new String[] {
											lit1.getExtension(),
											lit1.getValue(),
											lit2.getExtension(),
											lit2.getValue() }))
									|| FacetDbUtils.contains(m_cache,
											FacetDbUtils.getKey(new String[] {
													lit2.getExtension(),
													lit2.getValue(),
													lit1.getExtension(),
													lit1.getValue() }));

							if (!containsDistance) {

								DataType type2 = lit2.getDataType();
								double distance = Double.NaN;

								if ((type1 == type2)) {

									if ((type1 != DataType.UNKNOWN)
											&& (type2 != DataType.UNKNOWN)) {

										IDistanceMetric metric = DistanceMetricPool
												.getMetric(type1);

										distance = metric.getDistance(lit1,
												lit2);

									} else {

										s_log
												.debug("literal1 has type 'unknown' ("
														+ type1
														+ ") or literal2 has type 'unkown' ("
														+ type2 + ")");
									}
								} else {

									s_log.debug("literal1 has type '" + type1
											+ "' whereas literal2 has type '"
											+ type2 + "'");
								}

								// System.out.println("Distance:");
								// System.out.println("lit1:" + lit1 +
								// " / lit2:"
								// + lit2 + " dis:" + distance);
								// System.out.println();

								m_distanceIndex.addData(IndexDescription.ELELD,
										new String[] { lit1.getExtension(),
												lit1.getValue(),
												lit2.getExtension(),
												lit2.getValue() }, String
												.valueOf(distance));

								FacetDbUtils
										.store(m_cache, FacetDbUtils
												.getKey(new String[] {
														lit1.getExtension(),
														lit1.getValue(),
														lit2.getExtension(),
														lit2.getValue() }),
												DUMMY_ENTRY);

							}
							// else {
							//
							// s_log.debug("skipped literal "
							// + lit1.getValue() + " / literal "
							// + lit2.getValue() + "!");
							//
							// }
						}
					}

					s_log.debug("finished data-property " + prop.getValue()
							+ "!");
				}
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

		m_env = EnvironmentFactory.make(this.m_idxDirectory.getDirectory(
				IndexDirectory.FACET_DISTANCES_DIR, true));

		m_cache = this.m_env.openDatabase(null,
				FacetDbUtils.DatabaseNames.FDB_CACHE, DbConfigFactory
						.make(true));

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

					System.out.println("Sorted list:");
					System.out.println("---------------------------");

					for (ILiteral object : objects) {
						System.out.println("object: " + object.getValue());
						System.out.println();
					}

					System.out.println("---------------------------");

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
