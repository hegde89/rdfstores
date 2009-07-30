package edu.unika.aifb.graphindex.searcher.structured.sig;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public abstract class AbstractIndexGraphMatcher implements IndexGraphMatcher {

	protected IndexReader m_idxReader;
	protected QueryExecution m_qe;
	protected StructuredQuery m_query;
	protected QueryGraph m_queryGraph;

	protected Map<String,GTable<String>> m_p2to;
	protected Map<String,GTable<String>> m_p2ts;

	protected Timings m_timings;
	protected Counters m_counters;
	
	private final static Logger log = Logger.getLogger(AbstractIndexGraphMatcher.class);
	
	protected AbstractIndexGraphMatcher(IndexReader idxReader) throws IOException {
		m_idxReader = idxReader;

		m_timings = new Timings();
		m_counters = new Counters();
		
		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("this index matcher is incompatible with the index");
	}
	
	public void initialize() throws StorageException, IOException {
		m_p2ts = new HashMap<String,GTable<String>>();
		m_p2to = new HashMap<String,GTable<String>>();
		
		IndexStorage gs = m_idxReader.getStructureIndex().getGraphIndexStorage();
		
		int igedges = 0;
		for (String property : m_idxReader.getObjectProperties()) {
			m_p2ts.put(property, gs.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property));
			m_p2to.put(property, gs.getIndexTable(IndexDescription.PSO, DataField.SUBJECT, DataField.OBJECT, property));
		}
		
		log.debug("index graph edges: " + igedges);
		
		for (GTable<String> t : m_p2ts.values())
			t.sort(0);
		for (GTable<String> t : m_p2to.values())
			t.sort(1);
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
		m_query = qe.getQuery();
		m_queryGraph = qe.getQueryGraph();
	}
	
	protected abstract boolean isCompatibleWithIndex() throws IOException;

	public void setTimings(Timings timings) {
		m_timings = timings;
	}
	
	public void setCounters(Counters c) {
		m_counters = c;
	}
}
