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
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
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
	private KeywordIndex m_keywordIndex;
	private NeighborhoodStorage m_neighborhoodStorage;
	private Set<String> m_objectProperties;
	private Set<String> m_dataProperties;
	private boolean m_warmup = true;
	
	private StatisticsCollector m_collector;
	
	private static final Logger log = Logger.getLogger(IndexReader.class);
	
	public IndexReader(IndexDirectory idxDirectory) throws IOException {
		m_idxDirectory = idxDirectory;
		m_idxConfig = new IndexConfiguration();
		m_idxConfig.load(m_idxDirectory);
		
//		for (IndexDescription idx: m_idxConfig.getIndexes(IndexConfiguration.SP_INDEXES))
//			log.info("sp index: " + idx);
//		for (IndexDescription idx: m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES))
//			log.info("di index: " + idx);
		
		m_collector = new StatisticsCollector();
		
		log.debug(Util.memory());
	}
	
	public void enableWarmup(boolean warmup) {
		m_warmup = warmup;
	}
	
	public synchronized DataIndex getDataIndex() throws IOException, StorageException {
		if (m_dataIndex == null) 
			m_dataIndex = new DataIndex(m_idxDirectory, m_idxConfig, m_warmup);
		return m_dataIndex;
	}
	
	public StructureIndex getStructureIndex() throws IOException, StorageException {
		if (m_structureIndex == null)
			m_structureIndex = new StructureIndex(this, m_warmup);
		return m_structureIndex;
	}

	public IndexConfiguration getIndexConfiguration() {
		return m_idxConfig;
	}
	
	public IndexDirectory getIndexDirectory() {
		return m_idxDirectory;
	}
	
	public Set<String> getObjectProperties() throws IOException {
		if (m_objectProperties == null)
			m_objectProperties = Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE));
		return m_objectProperties;
	}
	
	public Set<String> getDataProperties() throws IOException {
		if (m_dataProperties == null)
			m_dataProperties = Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.DATA_PROPERTIES_FILE));
		return m_dataProperties;
	}
	
	public NeighborhoodStorage getNeighborhoodStorage() throws StorageException, IOException {
		if (m_neighborhoodStorage == null) {
			m_neighborhoodStorage = new LuceneNeighborhoodStorage(m_idxDirectory.getDirectory(IndexDirectory.NEIGHBORHOOD_DIR));
			m_neighborhoodStorage.initialize(false, true);
		}
		return m_neighborhoodStorage;
	}
	
	public int getSubjectCardinality(String property) {
		return 0;
	}

	public int getObjectCardinality(String property) {
		return 0;
	}

	public StatisticsCollector getCollector() {
		return m_collector;
	}
}
