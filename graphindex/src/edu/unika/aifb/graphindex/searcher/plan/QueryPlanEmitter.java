package edu.unika.aifb.graphindex.searcher.plan;

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
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.PrunedPartMatcher;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
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
public class QueryPlanEmitter extends StructuredQueryEvaluator {

	private PrunedPartMatcher m_matcher;
	private IndexStorage m_is;
	private DataIndex m_ds;
	private QueryExecution m_qe;
	private boolean m_doRefinement = true;
	private IndexDescription m_idxPOS;
	private IndexDescription m_idxPSO;
	private BlockCache m_bc;
	
	private static final Logger log = Logger.getLogger(QueryPlanEmitter.class);

	public QueryPlanEmitter(IndexReader idxReader) throws StorageException, IOException {
		super(idxReader);
		
		m_ds = idxReader.getDataIndex();
		m_is = idxReader.getStructureIndex().getSPIndexStorage();

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

	public String getQueryCode(StructuredQuery q) throws StorageException, IOException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		m_idxReader.getCollector().addTimings(timings);
		m_idxReader.getCollector().addCounters(counters);
		
		m_qe = new QueryExecution(q, m_idxReader);
//		m_matcher.setQueryExecution(m_qe);
		
		StringWriter sw = new StringWriter();
		PrintWriter out = new PrintWriter(sw);
		
		counters.set(Counters.QUERY_EDGES, q.getQueryGraph().edgeCount());
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		final QueryGraph queryGraph = m_qe.getQueryGraph();
		final Map<String,Integer> proximites = m_qe.getProximities();

		List<Table<String>> resultTables = m_qe.getResultTables() == null ? new ArrayList<Table<String>>() : m_qe.getResultTables(); 
		Map<Table<String>,String> tableNames = new HashMap<Table<String>,String>();
		Map<PrunedQueryPart,String> partNames = new HashMap<PrunedQueryPart,String>();
		int nameCount = 0;
		
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
		
		Set<String> selectNodes = new HashSet<String>(q.getSelectVariableLabels());
		Set<String> prevIncompleteNodes = new HashSet<String>();
		
		out.println("public void query_" + q.getName() + (m_doRefinement ? "_spc_" + m_idxReader.getIndexConfiguration().getInteger(IndexConfiguration.SP_PATH_LENGTH) : "_vp") + "() throws StorageException {");
		Map<String,PrunedQueryPart> prunedParts = new HashMap<String,PrunedQueryPart>();
		if (m_doRefinement) {
			int removedEdges = 0;
			int partCount = 0;
			for (PrunedQueryPart part : m_qe.getPrunedQuery().getPrunedParts()) {
				prunedParts.put(part.getRoot().getLabel(), part);
				removedEdges += part.getQueryGraph().edgeCount();
				partNames.put(part, "p" + ++partCount);
				
				out.println("PrunedQueryPart " + partNames.get(part) + " = new PrunedQueryPart(\"p\", null);");
				for (QueryEdge e : part.getQueryGraph().edgeSet())
					out.println(partNames.get(part) + ".addEdge(\"" + e.getSource().getLabel() + "\", \"" + e.getLabel() + "\", \"" + e.getTarget().getLabel() + "\");");
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
			
//			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
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

//			log.debug("src table: " + sourceTable + ", trg table: " + targetTable);
			
			String nextTable = "t" + ++nameCount;
			Table<String> result;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
//				result = joinWithTable(property, srcLabel, trgLabel, targetTable, m_idxPOS, DataField.OBJECT, targetTable.getColumn(trgLabel));
				List<String> columns = new ArrayList<String>();
				columns.addAll(targetTable.getColumnNamesSorted());
				columns.add(srcLabel);
				result = new Table<String>(columns);
				
				out.println("Table<String> " + nextTable + " = m_op.indexJoin(" + tableNames.get(targetTable) + ", \"" + trgLabel + "\", IndexDescription.POCS, SO, " +
					"names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.OBJECT, DataField.PROPERTY, \"" + property + "\");");
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
//				result = joinWithTable(property, srcLabel, trgLabel, sourceTable, m_idxPSO, DataField.SUBJECT, sourceTable.getColumn(srcLabel));
				List<String> columns = new ArrayList<String>();
				columns.addAll(sourceTable.getColumnNamesSorted());
				columns.add(trgLabel);
				result = new Table<String>(columns);
				
				if (!Util.isConstant(trgLabel)) {
					out.println("Table<String> " + nextTable + " = m_op.indexJoin(" + tableNames.get(sourceTable) + ", \"" + srcLabel + "\", IndexDescription.PSOC, SO, " +
						"names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.SUBJECT, DataField.PROPERTY, \"" + property + "\");");
				}
				else {
					out.println("Table<String> " + nextTable + "_a = m_op.load(IndexDescription.POCS, SO, names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.PROPERTY, \"" + property + "\", DataField.OBJECT, \"" + trgLabel + "\");");
					out.println("Table<String> " + nextTable + " = m_op.mergeJoin(" + nextTable + "_a, " + tableNames.get(sourceTable) + ", \"" + srcLabel + "\");");
				}
			}
			else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				if (Util.isConstant(trgLabel)) {
					result = new Table<String>(srcLabel, trgLabel);
					out.println("Table<String> " + nextTable + " = m_op.load(IndexDescription.POCS, SO, names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.PROPERTY, \"" + property + "\", DataField.OBJECT, \"" + trgLabel + "\");");
				}
				else
					throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
			}
			else {
				// case 3: both nodes already processed
//				result = evaluateBothMatched(property, srcLabel, trgLabel, sourceTable, targetTable);
				Set<String> columns = new HashSet<String>();
				columns.addAll(sourceTable.getColumnNamesSorted());
				columns.addAll(targetTable.getColumnNamesSorted());
				result = new Table<String>(new ArrayList<String>(columns));
				
				if (sourceTable == targetTable) {
					out.println("Table<String> " + nextTable + " = m_op.indexJoin(" + tableNames.get(sourceTable) + ", \"" + srcLabel + "\", \"" + trgLabel + "\", IndexDescription.PSOC, SO, " +
						"names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.SUBJECT, DataField.PROPERTY, \"" + property + "\");");
				}
				else {
					out.println("Table<String> " + nextTable + "_a = m_op.indexJoin(" + tableNames.get(sourceTable) + ", \"" + srcLabel + "\", IndexDescription.PSOC, SO, " +
						"names(\"" + srcLabel + "\", \"" + trgLabel + "\"), DataField.SUBJECT, DataField.PROPERTY, \"" + property + "\");");
					out.println("Table<String> " + nextTable + " = m_op.mergeJoin(" + nextTable + "_a, " + tableNames.get(targetTable) + ", \"" + trgLabel + "\");");
				}
			}
			
			
//			if (srcLabel.equals("?x4") && trgLabel.equals("?x3")) 
			if (m_doRefinement && prunedParts.containsKey(srcLabel) && !verified.contains(srcLabel)) {
//				log.debug("source refinement");
//				result = refineWithPrunedPart(prunedParts.get(srcLabel), srcLabel, result);
				verified.add(srcLabel);
				out.println(nextTable + " = m_op.refineWithPrunedPart(" + partNames.get(prunedParts.get(srcLabel)) + ", \"" + srcLabel + "\", " + nextTable + ");");
			}
			
			if (m_doRefinement && prunedParts.containsKey(trgLabel) && !verified.contains(trgLabel)) {
//				log.debug("target refinement");
//				result = refineWithPrunedPart(prunedParts.get(trgLabel), trgLabel, result);
				verified.add(trgLabel);
				out.println(nextTable + " = m_op.refineWithPrunedPart(" + partNames.get(prunedParts.get(trgLabel)) + ", \"" + trgLabel + "\", " + nextTable + ");");
			}
			
//			log.debug("res: " + result);
			
			resultTables.add(result);
			tableNames.put(result, nextTable);
			
			m_qe.visited(currentEdge);
			
			Set<String> incomplete = new HashSet<String>(selectNodes);
			for (QueryEdge edge : toVisit) {
				incomplete.add(edge.getSource().getLabel());
				incomplete.add(edge.getTarget().getLabel());
			}

			if (!incomplete.equals(prevIncompleteNodes) && result.columnCount() > 2) {
				String cols = "";
				String comma = "";
				for (String col : incomplete) {
					if (result.hasColumn(col)) {
						cols += comma + "\"" + col + "\"";
						comma = ", ";
					}
				}
				
				out.println(nextTable + " = m_op.compact(" + nextTable + ", Arrays.asList(" + cols + "));");
			}
			prevIncompleteNodes = incomplete;

//			log.debug("");
			out.println();
			counters.inc(Counters.DM_PROCESSED_EDGES);
		}
		
//		String cols = "";
//		String comma = "";
//		for (String col : q.getSelectVariableLabels()) {
//			cols += comma + "\"" + col + "\"";
//			comma = ", ";
//		}
//		out.println("m_op.compact(t" + nameCount + ", Arrays.asList(" + cols + "));");

		out.println("}");
		m_qe.addResult(resultTables.get(0), true);
		
		timings.end(Timings.TOTAL_QUERY_EVAL);
		
		counters.set(Counters.RESULTS, m_qe.getResult().rowCount());
		
		return sw.toString();
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}

	@Override
	public Table<String> evaluate(StructuredQuery q) throws StorageException, IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
