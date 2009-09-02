package edu.unika.aifb.graphindex.index;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
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
 */

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.facets.index.FacetIndex;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneNeighborhoodStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;

/**
 * IndexReader provides an interface for accessing a graphindex.
 * 
 * @author gla
 */
public class IndexReader {
	private IndexDirectory m_idxDirectory;
	private IndexConfiguration m_idxConfig;

	private DataIndex m_dataIndex;
	private StructureIndex m_structureIndex;
	private FacetIndex m_facetIndex;
	private NeighborhoodStorage m_neighborhoodStorage;
	private Set<String> m_objectProperties;
	private Set<String> m_dataProperties;

	private StatisticsCollector m_collector;

	private static final Logger log = Logger.getLogger(IndexReader.class);

	public IndexReader(IndexDirectory idxDirectory) throws IOException {
		this.m_idxDirectory = idxDirectory;
		this.m_idxConfig = new IndexConfiguration();
		this.m_idxConfig.load(this.m_idxDirectory);

		for (IndexDescription idx : this.m_idxConfig
				.getIndexes(IndexConfiguration.SP_INDEXES)) {
			log.info("sp index: " + idx);
		}
		for (IndexDescription idx : this.m_idxConfig
				.getIndexes(IndexConfiguration.DI_INDEXES)) {
			log.info("di index: " + idx);
		}

		this.m_collector = new StatisticsCollector();
	}

	public StatisticsCollector getCollector() {
		return this.m_collector;
	}

	public DataIndex getDataIndex() throws IOException {
		if (this.m_dataIndex == null) {
			this.m_dataIndex = new DataIndex(this.m_idxDirectory,
					this.m_idxConfig);
		}
		return this.m_dataIndex;
	}

	public Set<String> getDataProperties() throws IOException {
		if (this.m_dataProperties == null) {
			this.m_dataProperties = new HashSet<String>();
			m_dataProperties.addAll(Util.readEdgeSet(this.m_idxDirectory
					.getFile(IndexDirectory.DATA_PROPERTIES_FILE)));
		}
		return this.m_dataProperties;
	}

	public FacetIndex getFacetIndex() {

		this.m_facetIndex = null;

		try {
			if (this.m_facetIndex == null) {
				this.m_facetIndex = new FacetIndex(this.m_idxDirectory,
						this.m_idxConfig);
			}
		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return this.m_facetIndex;
	}

	public IndexConfiguration getIndexConfiguration() {
		return this.m_idxConfig;
	}

	public IndexDirectory getIndexDirectory() {
		return this.m_idxDirectory;
	}

	public NeighborhoodStorage getNeighborhoodStorage()
			throws StorageException, IOException {
		if (this.m_neighborhoodStorage == null) {
			this.m_neighborhoodStorage = new LuceneNeighborhoodStorage(
					this.m_idxDirectory
							.getDirectory(IndexDirectory.NEIGHBORHOOD_DIR));
			this.m_neighborhoodStorage.initialize(false, true);
		}
		return this.m_neighborhoodStorage;
	}

	public int getObjectCardinality(String property) {
		return 0;
	}

	public Set<String> getObjectProperties() throws IOException {
		if (this.m_objectProperties == null) {
			this.m_objectProperties = new HashSet<String>();
			this.m_objectProperties.addAll(Util.readEdgeSet(this.m_idxDirectory
					.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE)));
		}
		return this.m_objectProperties;
	}

	public StructureIndex getStructureIndex() throws IOException {
		if (this.m_structureIndex == null) {
			this.m_structureIndex = new StructureIndex(this.m_idxDirectory,
					this.m_idxConfig);
		}
		return this.m_structureIndex;
	}

	public int getSubjectCardinality(String property) {
		return 0;
	}
}
