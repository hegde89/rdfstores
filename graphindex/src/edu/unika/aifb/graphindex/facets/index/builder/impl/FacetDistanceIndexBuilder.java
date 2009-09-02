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
package edu.unika.aifb.graphindex.facets.index.builder.impl;

import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Util;

import edu.unika.aifb.graphindex.facets.index.builder.IFacetIndexBuilder;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;

/**
 * @author andi
 * 
 */
public class FacetDistanceIndexBuilder implements IFacetIndexBuilder {

	private IndexReader m_idxReader;
	private IndexDirectory m_idxDirectory;
	private FacetIndexBuilderHelper m_helper;
	private LuceneIndexStorage m_distanceIndex;
	private DistanceCalculator m_calc;

	public FacetDistanceIndexBuilder(IndexDirectory idxDirectory,
			IndexReader idxReader, FacetIndexBuilderHelper helper) {

		this.m_idxReader = idxReader;
		this.m_idxDirectory = idxDirectory;
		this.m_helper = helper;

		this.m_calc = new DistanceCalculator();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.graphindex.algorithm.facets.index.builder.IFacetIndexBuilder
	 * #build()
	 */
	@Override
	public void build() throws Exception {
		
		this.m_distanceIndex = new LuceneIndexStorage(this.m_idxDirectory
				.getDirectory(IndexDirectory.FACET_DISTANCES_DIR, true));

		this.m_distanceIndex.initialize(true, false);

		DirectedMultigraph<NodeElement, EdgeElement> idxGraph = this.m_helper
				.getIndexGraph();

		Set<NodeElement> extensions = idxGraph.vertexSet();

		for (NodeElement extension : extensions) {

			List<String> subjects = null;
			
//			TODO

			int count = 0;

			for (String subject1 : subjects) {

				if (Util.isLiteral(subject1)) {

					for (String subject2 : subjects) {

						if (Util.isLiteral(subject2)) {

							this.m_distanceIndex.addData(IndexDescription.ELLD,
									new String[] { extension.getLabel(),
											subject1, subject2 }, Integer
											.toString(count++));
						}
					}
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.graphindex.algorithm.facets.index.builder.IFacetIndexBuilder
	 * #close()
	 */
	@Override
	public void close() throws Exception {

		this.m_distanceIndex.optimize();
		this.m_distanceIndex.close();

	}

	private class DistanceCalculator {

		public double getStringSimilarity(String s1, String s2) {

			double sim = -1.0;
			
//			TODO
			
			return sim;
		}
	}
}
