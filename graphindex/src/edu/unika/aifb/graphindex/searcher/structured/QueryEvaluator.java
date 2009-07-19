package edu.unika.aifb.graphindex.searcher.structured;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.sig.IndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.sig.IndexMatchesValidator;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryEvaluator extends StructuredQueryEvaluator {
	private Timings m_timings;
	private IndexGraphMatcher m_matcher;
	private IndexMatchesValidator m_mlv;
	private Counters m_counters;
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public QueryEvaluator(IndexReader indexReader) throws IOException, StorageException {
		super(indexReader);

		m_mlv = new SmallIndexMatchesValidator(indexReader);

		m_matcher = new SmallIndexGraphMatcher(indexReader);
		m_matcher.initialize();
	}
	
	public IndexMatchesValidator getMLV() {
		return m_mlv;
	}
	
	public GTable<String> evaluate(StructuredQuery query) throws StorageException, IOException {
		log.info("evaluating...");

		m_idxReader.getCollector().clear();
		m_timings = new Timings();
		m_counters = new Counters();
		m_idxReader.getCollector().addTimings(m_timings);
		m_idxReader.getCollector().addCounters(m_counters);
		
		m_counters.set(Counters.QUERY_EDGES, query.getQueryGraph().edgeCount());
		m_timings.start(Timings.TOTAL_QUERY_EVAL);
		
		List<String[]> result = new ArrayList<String[]>();
		m_timings.start(Timings.STEP_IM);
		QueryExecution qe = new QueryExecution(query, m_idxReader);
		
		m_matcher.setTimings(m_timings);
		m_matcher.setCounters(m_counters);
		m_matcher.setQueryExecution(qe);

		GTable.timings = m_timings;
		Tables.timings = m_timings;
		
		m_matcher.match();
		m_timings.end(Timings.STEP_IM);
		
//		m_counters.set(Counters.IM_INDEX_MATCHES, qe.getIndexMatches().rowCount());
		
		m_mlv.setTimings(m_timings);
		m_mlv.setCounters(m_counters);
		m_mlv.setQueryExecution(qe);
		
		m_timings.start(Timings.STEP_DM);
		m_mlv.validateIndexMatches();

		qe.finished();
		m_timings.end(Timings.STEP_DM);
		
		m_timings.end(Timings.TOTAL_QUERY_EVAL);
		
		m_counters.set(Counters.RESULTS, result.size());
		log.debug("size: " + result.size());
		
		return qe.getResult();
	}
	
	public void clearCaches() throws StorageException {
		m_mlv.clearCaches();
	}
}
