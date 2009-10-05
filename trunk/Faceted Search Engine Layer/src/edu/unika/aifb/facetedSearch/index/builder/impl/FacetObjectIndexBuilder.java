/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.index.builder.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
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
public class FacetObjectIndexBuilder implements IFacetIndexBuilder {

	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;
	private FacetIdxBuilderHelper m_helper;
	private LuceneIndexStorage m_objectIndex;
	// private LuceneIndexStorage m_sortedLitIndex;
	// private LiteralComparator m_litComparator;

	private final static Logger s_log = Logger
			.getLogger(FacetObjectIndexBuilder.class);

	public FacetObjectIndexBuilder(IndexDirectory idxDirectory,
			IndexReader idxReader, FacetIdxBuilderHelper helper) {

		m_idxReader = idxReader;
		m_idxDirectory = idxDirectory;
		m_helper = helper;
		// m_litComparator = new LiteralComparator();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder#build()
	 */
	@SuppressWarnings("null")
	public void build() throws IOException, StorageException, DatabaseException {

		IndexStorage spIdx = m_idxReader.getStructureIndex()
				.getSPIndexStorage();

		Set<NodeElement> source_extensions = m_helper.getIndexGraph()
				.vertexSet();

		m_objectIndex = new LuceneIndexStorage(m_idxDirectory.getDirectory(
				IndexDirectory.FACET_OBJECTS_DIR, true), m_idxReader
				.getCollector());

		m_objectIndex.initialize(true, false);

		// m_sortedLitIndex = new
		// LuceneIndexStorage(m_idxDirectory.getDirectory(
		// IndexDirectory.FACET_LITERALS_DIR, true), m_idxReader
		// .getCollector());
		//
		// m_sortedLitIndex.initialize(true, false);

		int count = 0;

		for (NodeElement source_extension : source_extensions) {

			s_log.debug("start building object index for extension: "
					+ source_extension + " (" + (++count) + "/"
					+ source_extensions.size() + ")");

			List<String> individuals = spIdx.getDataList(
					IndexDescription.EXTENT, DataField.ENT, source_extension
							.getLabel());

			s_log.debug("extension contains " + individuals.size()
					+ " individuals.");

			HashMap<Node, HashSet<String>> endPoints = null;
			// m_helper
			// .getEndPoints(source_extension.getLabel());

			String[] key;
			String[] values;

			for (Entry<Node, HashSet<String>> endpointEntry : endPoints
					.entrySet()) {

				List<String> rangeExtensions = new ArrayList<String>();
				// List<String> allObjects = new ArrayList<String>();
				Node property = endpointEntry.getKey();

				for (String individual : individuals) {

					List<String> objects4ind = new ArrayList<String>();

					try {

						Table<String> triples = m_idxReader.getDataIndex()
								.getTriples(individual, property.getValue(),
										null);

						Iterator<String[]> tripleIter = triples.getRows()
								.iterator();

						while (tripleIter.hasNext()) {

							String object = tripleIter.next()[2];
							String rangeExtension = m_helper
									.getExtension(object);

							// skip rdf type properties
							// if (property.getContent() !=
							// NodeContent.TYPE_PROPERTY) {

							objects4ind.add(object);
							rangeExtensions.add(rangeExtension);

							// if (!allObjects.contains(object)) {
							// allObjects.add(object);
							// }
							// }
						}
					} catch (StorageException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (NullPointerException e) {
						e.printStackTrace();
					}

					key = new String[] { individual,
							String.valueOf(property.getID()) };

					values = new String[] {
							FacetUtils.list2String(objects4ind),
							FacetUtils.list2String(rangeExtensions) };

					// add data to index
					// m_objectIndex.addDataWithMultipleValues(
					// IndexDescription.IEOE, key, values);
				}

				// key = new String[] { String.valueOf(property.getID()) };
				// Collections.sort(allObjects, m_litComparator);
				//
				// // add data to index
				// m_sortedLitIndex.addData(IndexDescription.EL, key, FacetUtils
				// .list2String(allObjects));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder#close()
	 */
	public void close() throws StorageException {

		m_objectIndex.optimize();
		m_objectIndex.close();

		// m_sortedLitIndex.optimize();
		// m_sortedLitIndex.close();

	}
}
