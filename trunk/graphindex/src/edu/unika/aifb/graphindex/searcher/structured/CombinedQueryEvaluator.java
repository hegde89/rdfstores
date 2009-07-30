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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

/**
 * CombinedQueryEvaluator evaluates structured queries using both the data and the structure index.
 *
 * @author gla
 */
public class CombinedQueryEvaluator extends StructuredQueryEvaluator {

	private PrunedPartMatcher m_matcher;
	private IndexStorage m_is, m_ds;
	private QueryExecution m_qe;
	private boolean m_doRefinement;
	
	private static final Logger log = Logger.getLogger(CombinedQueryEvaluator.class);

	public CombinedQueryEvaluator(IndexReader idxReader) throws StorageException, IOException {
		super(idxReader);
		
		m_matcher = new PrunedPartMatcher(idxReader);
		m_matcher.initialize();
		
		m_ds = idxReader.getDataIndex().getIndexStorage();
		m_is = idxReader.getStructureIndex().getSPIndexStorage();
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
	}
	
	public void setDoRefinement(boolean doRefinement) {
		m_doRefinement = doRefinement;
	}

	public GTable<String> evaluate(StructuredQuery q) throws StorageException, IOException {

		Timings timings = new Timings();
		Counters counters = new Counters();
//		m_index.getCollector().addTimings(timings);
//		m_index.getCollector().addCounters(counters);
		
		m_qe = new QueryExecution(q, m_idxReader);
		m_matcher.setQueryExecution(m_qe);
		
		counters.set(Counters.QUERY_EDGES, q.getQueryGraph().edgeCount());
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		final QueryGraph queryGraph = m_qe.getQueryGraph();
		final Map<String,Integer> proximites = m_qe.getProximities();

		List<GTable<String>> resultTables = m_qe.getResultTables() == null ? new ArrayList<GTable<String>>() : m_qe.getResultTables(); 

		Queue<QueryEdge> toVisit = new PriorityQueue<QueryEdge>(queryGraph.edgeCount(), new Comparator<QueryEdge>() {
			public int compare(QueryEdge e1, QueryEdge e2) {
				String s1 = e1.getSource().getLabel();
				String s2 = e2.getSource().getLabel();
				String d1 = e1.getTarget().getLabel();
				String d2 = e2.getTarget().getLabel();
								
				int e1score = proximites.get(s1) * proximites.get(d1);
				int e2score = proximites.get(s2) * proximites.get(d2);
				
				if (e1score == e2score) {
					Integer ce1 = m_idxReader.getObjectCardinality(e1.getLabel());
					Integer ce2 = m_idxReader.getObjectCardinality(e2.getLabel());
					
					if (ce1 != null && ce2 != null && ce1.intValue() != ce2.intValue()) {
						if (ce1 < ce2)
							return 1;
						else
							return -1;
					}
				}
				
				if (e1score < e2score)
					return -1;
				else
					return 1;
			}
		});

		if (m_doRefinement)
			toVisit.addAll(m_qe.getPrunedQuery().getQueryGraph().edgeSet());
		else
			toVisit.addAll(m_qe.toVisit());
		

		Map<String,PrunedQueryPart> prunedParts = new HashMap<String,PrunedQueryPart>();
		if (m_doRefinement) {
			for (PrunedQueryPart part : m_qe.getPrunedQuery().getPrunedParts()) {
				prunedParts.put(part.getRoot().getLabel(), part);
			}
		
//			counters.set(Counters.DM_REM_EDGES, q.getPrunedEdges().size());
//			counters.set(Counters.DM_REM_NODES, q.getRemovedNodes().size());
		}
		
		Set<String> visited = new HashSet<String>();
		Set<String> verified = new HashSet<String>();
		
		while (toVisit.size() > 0) {
			String property, srcLabel, trgLabel;
			QueryEdge currentEdge;
			
			List<QueryEdge> skipped = new ArrayList<QueryEdge>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcLabel = currentEdge.getSource().getLabel();
				trgLabel = currentEdge.getTarget().getLabel();
			}
			while ((!visited.contains(srcLabel) && !visited.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel)));

			visited.add(srcLabel);
			visited.add(trgLabel);
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			GTable<String> sourceTable = null, targetTable = null;
			for (GTable<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}
			
			if (Util.isConstant(trgLabel))
				targetTable = null;
			
			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			log.debug("src table: " + sourceTable + ", trg table: " + targetTable);

			GTable<String> result;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				result = joinWithTable(property, srcLabel, trgLabel, targetTable, IndexDescription.POS, targetTable.getColumn(trgLabel));
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				result = joinWithTable(property, srcLabel, trgLabel, sourceTable, IndexDescription.PSO, sourceTable.getColumn(srcLabel));
			}
			else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				result = evaluateBothUnmatched(property, srcLabel, trgLabel);
			}
			else {
				// case 3: both nodes already processed
				result = evaluateBothMatched(property, srcLabel, trgLabel, sourceTable, targetTable);
			}
			
			if (m_doRefinement && prunedParts.containsKey(srcLabel) && !verified.contains(srcLabel)) {
				result = refineWithPrunedPart(prunedParts.get(srcLabel), srcLabel, result);
				verified.add(srcLabel);
			}
			
			if (m_doRefinement && prunedParts.containsKey(trgLabel) && !verified.contains(trgLabel)) {
				result = refineWithPrunedPart(prunedParts.get(trgLabel), trgLabel, result);
				verified.add(trgLabel);
			}
			
			log.debug("res: " + result);
			
			resultTables.add(result);
			m_qe.visited(currentEdge);

			log.debug("");
			counters.inc(Counters.DM_PROCESSED_EDGES);
		}
		log.debug(resultTables);
		m_qe.addResult(resultTables.get(0), true);
		
		timings.end(Timings.TOTAL_QUERY_EVAL);
		
		counters.set(Counters.RESULTS, m_qe.getResult().rowCount());
		
		return m_qe.getResult();
	}

	private GTable<String> refineWithPrunedPart(PrunedQueryPart prunedQueryPart, String srcLabel, GTable<String> result) throws StorageException {
		Map<String,List<String[]>> ext2entity = new HashMap<String,List<String[]>>(100);
		GTable<String> extTable = new GTable<String>(Arrays.asList(srcLabel));
		
		int col = result.getColumn(srcLabel);
		
		Set<String> values = new HashSet<String>();
		for (String[] row : result) {
			String ext = m_is.getDataItem(IndexDescription.SES, DataField.EXT_SUBJECT, row[col]);
			if (values.add(ext))
				extTable.addRow(new String[] { ext });
			
			List<String[]> entities = ext2entity.get(ext);
			if (entities == null) {
				entities = new ArrayList<String[]>(200);
				ext2entity.put(ext, entities);
			}
			entities.add(row);
		}
		
		m_matcher.setPrunedPart(prunedQueryPart, srcLabel, extTable);
		m_matcher.match();
		
		int rows = result.rowCount();
		result = new GTable<String>(result, false);
		for (String ext : m_matcher.getValidExtensions()) {
			result.addRows(ext2entity.get(ext));
		}
		log.debug(" refined: " + rows + " => " + result.rowCount());
		
		return result;
	}
	
	private GTable<String> joinWithTable(String property, String srcLabel, String trgLabel, GTable<String> table, IndexDescription index, int col) throws StorageException, IOException {
		GTable<String> t2 = new GTable<String>(srcLabel, trgLabel);
		
		boolean targetConstant = Util.isConstant(trgLabel);
		Set<String> values = new HashSet<String>();
		for (String[] row : table) {
			if (values.add(row[col])) {
				 GTable<String> t3 = m_ds.getIndexTable(index, DataField.SUBJECT, DataField.OBJECT, property, row[col]);
				 if (!targetConstant)
					 t2.addRows(t3.getRows());
				 else {
					 for (String[] t3row : t3) {
						 if (t3row[1].equals(trgLabel))
							 t2.addRow(t3row);
					 }
				 }
			}
		}
		log.debug("unique values: " + values.size());
		
		String joinCol = table.getColumnName(col);
		
		table.sort(joinCol, true);
		t2.sort(joinCol, true);

		GTable<String> result = Tables.mergeJoin(table, t2, joinCol);
		
		return result;
	}
	
	private GTable<String> evaluateBothUnmatched(String property, String srcLabel, String trgLabel) throws StorageException, IOException {
		if (Util.isConstant(trgLabel)) {
			GTable<String> table = m_ds.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property, trgLabel);
			table.setColumnName(0, srcLabel);
			table.setColumnName(1, trgLabel);
			return table;
		}
		else
			throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
	}
	
	private GTable<String> evaluateBothMatched(String property, String srcLabel, String trgLabel, GTable<String> sourceTable, GTable<String> targetTable) throws StorageException, IOException {
		GTable<String> table;
		if (sourceTable == targetTable) {
			int col = sourceTable.getColumn(srcLabel);
			Set<String> values = new HashSet<String>();
			GTable<String> t2 = new GTable<String>(srcLabel, trgLabel);
			for (String[] row : sourceTable) {
				if (values.add(row[col]))
					t2.addRows(m_ds.getIndexTable(IndexDescription.PSO, DataField.SUBJECT, DataField.OBJECT, property, row[col]).getRows());
			}
			log.debug("unique values: " + values.size());
			table = Tables.mergeJoin(sourceTable, t2, Arrays.asList(srcLabel, trgLabel));
		}
		else {
			table = joinWithTable(property, srcLabel, trgLabel, sourceTable, IndexDescription.PSO, sourceTable.getColumn(srcLabel));
			
			table.sort(trgLabel, true);
			targetTable.sort(trgLabel, true);
			
			table = Tables.mergeJoin(table, targetTable, trgLabel);
		}
		
		return table;
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}
