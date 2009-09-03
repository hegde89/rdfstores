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
import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.facetedSearch.index.builder.IFacetIndexBuilder;
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
public class FacetVPosIndexBuilder implements IFacetIndexBuilder{

	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;
	private FacetIndexBuilderHelper m_helper;
	private LuceneIndexStorage m_vPosIndex;

	public FacetVPosIndexBuilder(IndexDirectory idxDirectory,
			IndexReader idxReader, FacetIndexBuilderHelper helper) {

		this.m_idxReader = idxReader;
		this.m_idxDirectory = idxDirectory;
		this.m_helper = helper;

	}

	public void build() throws IOException, StorageException {

		IndexStorage spIdx = this.m_idxReader.getStructureIndex()
				.getSPIndexStorage();

		this.m_vPosIndex = new LuceneIndexStorage(this.m_idxDirectory
				.getDirectory(IndexDirectory.FACET_VPOS_DIR, true));

		this.m_vPosIndex.initialize(true, false);

		DirectedMultigraph<NodeElement, EdgeElement> idxGraph = this.m_helper
				.getIndexGraph();

		Set<NodeElement> extensions = idxGraph.vertexSet();

		for (NodeElement extension : extensions) {
			
			List<String> subjects = spIdx.getDataList(
					IndexDescription.EXTENT, DataField.ENT, extension
							.getLabel());

			int count = 0;

			for (String subject : subjects) {
				this.m_vPosIndex.addData(IndexDescription.ESV, new String[] {
						extension.getLabel(), subject }, Integer.toString(count++));
			}
		}

	}

	public void close() throws StorageException {
		this.m_vPosIndex.optimize();
		this.m_vPosIndex.close();
	}
}
