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

import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.Util;

public class StructureIndex extends Index {

	private Set<String> m_backwardSet, m_forwardSet;
	private LuceneIndexStorage m_gs;
	
	public StructureIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig) throws IOException {
		super(idxDirectory, idxConfig);
		
		m_backwardSet = Util.readEdgeSet(idxDirectory.getFile(IndexDirectory.BW_EDGESET_FILE));
		m_forwardSet = Util.readEdgeSet(idxDirectory.getFile(IndexDirectory.FW_EDGESET_FILE));
	}
	
	public IndexDescription getCompatibleIndex(DataField... fields) {
		for (IndexDescription index : m_idxConfig.getIndexes(IndexConfiguration.SP_INDEXES))
			if (index.isCompatible(fields))
				return index;
		return null;
	}

	public Set<String> getBackwardEdges() {
		return m_backwardSet;
	}

	public Set<String> getForwardEdges() {
		return m_forwardSet;
	}

	public int getPathLength() {
		return m_idxConfig.getInteger(IndexConfiguration.SP_PATH_LENGTH);
	}

	public IndexStorage getSPIndexStorage() throws IOException {
		if (m_is == null) {
			m_is = new LuceneIndexStorage(m_idxDirectory.getDirectory(IndexDirectory.SP_IDX_DIR));
			m_is.initialize(false, true);
		}
		return m_is;
	}
	
	public String getExtension(String node) throws StorageException, IOException {
		return getSPIndexStorage().getDataItem(IndexDescription.SES, node);
	}

	public IndexStorage getGraphIndexStorage() throws IOException {
		if (m_gs == null) {
			m_gs = new LuceneIndexStorage(m_idxDirectory.getDirectory(IndexDirectory.SP_GRAPH_DIR));
			m_gs.initialize(false, true);
		}
		return m_gs;
	}
}
