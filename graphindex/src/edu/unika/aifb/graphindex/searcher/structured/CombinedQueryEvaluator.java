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

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.DataIndex;
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
 * CombinedQueryEvaluator evaluates structured queries using both, data and structure index.
 *
 * @author gla
 */
public class CombinedQueryEvaluator extends StructuredQueryEvaluator {

	private PrunedPartMatcher m_matcher;
	private IndexStorage m_is;
	private DataIndex m_ds;
	private QueryExecution m_qe;
	private boolean m_doRefinement = true;
	private IndexDescription m_idxPOS;
	private IndexDescription m_idxPSO;
	private BlockCache m_bc;
	
	private static final Logger log = Logger.getLogger(CombinedQueryEvaluator.class);

	public CombinedQueryEvaluator(IndexReader idxReader) throws StorageException, IOException {
		super(idxReader);
		
		m_matcher = new PrunedPartMatcher(idxReader);
		m_matcher.initialize();
		
		m_ds = idxReader.getDataIndex();
		m_is = idxReader.getStructureIndex().getSPIndexStorage();
//		try {
//			m_bc = idxReader.getStructureIndex().getBlockCache();
//		} catch (DatabaseException e) {
//			throw new StorageException(e);
//		}

		m_idxPSO = idxReader.getDataIndex().getSuitableIndex(DataField.PROPERTY, DataField.SUBJECT);
		m_idxPOS = idxReader.getDataIndex().getSuitableIndex(DataField.PROPERTY, DataField.OBJECT);
		
		log.debug("pso index: " + m_idxPSO + ", pos index: " + m_idxPOS);
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
	}
	
	public void setDoRefinement(boolean doRefinement) {
		m_doRefinement = doRefinement;
	}

	public Table<String> evaluate(StructuredQuery q) throws StorageException, IOException {

		Timings timings = new Timings();
		Counters counters = new Counters();
		m_idxReader.getCollector().addTimings(timings);
		m_idxReader.getCollector().addCounters(counters);
		
		m_qe = new QueryExecution(q, m_idxReader);
		m_matcher.setQueryExecution(m_qe);
		
		counters.set(Counters.QUERY_EDGES, q.getQueryGraph().edgeCount());
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		final QueryGraph queryGraph = m_qe.getQueryGraph();
		final Map<String,Integer> proximites = m_qe.getProximities();

		List<Table<String>> resultTables = m_qe.getResultTables() == null ? new ArrayList<Table<String>>() : m_qe.getResultTables(); 

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
			toVisit.addAll(m_qe.getPrunedQuery().getPrunedQueryGraph().edgeSet());
		else
			toVisit.addAll(m_qe.toVisit());
		

		Map<String,PrunedQueryPart> prunedParts = new HashMap<String,PrunedQueryPart>();
		if (m_doRefinement) {
			int removedEdges = 0;
			for (PrunedQueryPart part : m_qe.getPrunedQuery().getPrunedParts()) {
				prunedParts.put(part.getRoot().getLabel(), part);
				removedEdges += part.getQueryGraph().edgeCount();
			}
			m_counters.set(Counters.DM_REM_EDGES, removedEdges);
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

			skipped.remove(currentEdge);
			toVisit.addAll(skipped);

			visited.add(srcLabel);
			visited.add(trgLabel);
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			Table<String> sourceTable = null, targetTable = null;
			for (Table<String> table : resultTables) {
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

			Table<String> result;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				result = joinWithTable(property, srcLabel, trgLabel, targetTable, m_idxPOS, DataField.OBJECT, targetTable.getColumn(trgLabel));
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				result = joinWithTable(property, srcLabel, trgLabel, sourceTable, m_idxPSO, DataField.SUBJECT, sourceTable.getColumn(srcLabel));
			}
			else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				result = evaluateBothUnmatched(property, srcLabel, trgLabel);
			}
			else {
				// case 3: both nodes already processed
				result = evaluateBothMatched(property, srcLabel, trgLabel, sourceTable, targetTable);
			}
			
			
//			if (srcLabel.equals("?x4") && trgLabel.equals("?x3")) 
			if (m_doRefinement && prunedParts.containsKey(srcLabel) && !verified.contains(srcLabel)) {
				log.debug("source refinement");
				result = refineWithPrunedPart(prunedParts.get(srcLabel), srcLabel, result);
				verified.add(srcLabel);
			}
			
			if (m_doRefinement && prunedParts.containsKey(trgLabel) && !verified.contains(trgLabel)) {
				log.debug("target refinement");
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
		
	//	m_qe = null;
		
		return m_qe.getResult();
	}

	private Table<String> refineWithPrunedPart(PrunedQueryPart prunedQueryPart, String srcLabel, Table<String> result) throws StorageException {
		m_timings.start(Timings.STEP_CB_REFINE);
		
		Map<String,List<String[]>> ext2entity = new HashMap<String,List<String[]>>(100);
		Table<String> extTable = new Table<String>(Arrays.asList(srcLabel));
		
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
		result = new Table<String>(result, false);
		for (String ext : m_matcher.getValidExtensions()) {
			result.addRows(ext2entity.get(ext));
		}
		log.debug(" refined: " + rows + " => " + result.rowCount());
		
		m_timings.end(Timings.STEP_CB_REFINE);
		return result;
	}
	
	private Table<String> joinWithTable(String property, String srcLabel, String trgLabel, Table<String> table, IndexDescription index, DataField df, int col) throws StorageException, IOException {
		Table<String> t2 = new Table<String>(srcLabel, trgLabel);
		
		boolean targetConstant = Util.isConstant(trgLabel);
		Set<String> values = new HashSet<String>();
		for (String[] row : table) {
			if (values.add(row[col])) {
				 Table<String> t3 = m_ds.getIndexStorage(index).getTable(index, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, index.createValueArray(DataField.PROPERTY, property, df, row[col]));
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

		Table<String> result = Tables.mergeJoin(table, t2, joinCol);
		
		return result;
	}
	
	private Table<String> evaluateBothUnmatched(String property, String srcLabel, String trgLabel) throws StorageException, IOException {
		if (Util.isConstant(trgLabel)) {
//			GTable<String> table = m_ds.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property, trgLabel);
			Table<String> table = m_ds.getIndexStorage(m_idxPOS).getTable(m_idxPOS, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, m_idxPOS.createValueArray(DataField.PROPERTY, property, DataField.OBJECT, trgLabel));
			table.setColumnName(0, srcLabel);
			table.setColumnName(1, trgLabel);
			return table;
		}
		else
			throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
	}
	
	private Table<String> evaluateBothMatched(String property, String srcLabel, String trgLabel, Table<String> sourceTable, Table<String> targetTable) throws StorageException, IOException {
		Table<String> table;
		if (sourceTable == targetTable) {
			int col = sourceTable.getColumn(srcLabel);
			Set<String> values = new HashSet<String>();
			Table<String> t2 = new Table<String>(srcLabel, trgLabel);
			for (String[] row : sourceTable) {
				if (values.add(row[col]))
					t2.addRows(m_ds.getIndexStorage(m_idxPSO).getTable(m_idxPSO, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, m_idxPSO.createValueArray(DataField.PROPERTY, property, DataField.SUBJECT, row[col])).getRows());
//					t2.addRows(m_ds.getIndexTable(IndexDescription.PSO, DataField.SUBJECT, DataField.OBJECT, property, row[col]).getRows());
			}
			log.debug("unique values: " + values.size());
			table = Tables.mergeJoin(sourceTable, t2, Arrays.asList(srcLabel, trgLabel));
		}
		else {
			table = joinWithTable(property, srcLabel, trgLabel, sourceTable, m_idxPSO, DataField.SUBJECT, sourceTable.getColumn(srcLabel));
			
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
