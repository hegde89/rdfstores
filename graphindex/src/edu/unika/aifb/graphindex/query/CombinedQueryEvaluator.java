package edu.unika.aifb.graphindex.query;

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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.LuceneStorage.Index;

public class CombinedQueryEvaluator implements IQueryEvaluator {

	private StructureIndex m_index;
	private LuceneStorage m_ls;
	private PrunedPartMatcher m_matcher;
	private QueryExecution m_qe;
	private ExtensionStorage m_es;
	private boolean m_doRefinement;
	
	private static final Logger log = Logger.getLogger(CombinedQueryEvaluator.class);

	public CombinedQueryEvaluator(StructureIndexReader reader, LuceneStorage ls) throws StorageException {
		m_index = reader.getIndex();
		m_es = m_index.getExtensionManager().getExtensionStorage();
		m_ls = ls;
		
		for (String ig : reader.getGraphNames()) {
			m_matcher = new PrunedPartMatcher(m_index, ig);
			m_matcher.initialize();
			break;
		}
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
	}
	
	public void setDoRefinement(boolean doRefinement) {
		m_doRefinement = doRefinement;
	}

	public List<String[]> evaluate(Query q) throws StorageException, IOException {

		Timings timings = new Timings();
		Counters counters = new Counters();
		m_index.getCollector().addTimings(timings);
		m_index.getCollector().addCounters(counters);
		
		m_qe = new QueryExecution(q, m_index);
		m_matcher.setQueryExecution(m_qe);
		
		counters.set(Counters.QUERY_EDGES, q.getLiterals().size());
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		final Graph<QueryNode> queryGraph = m_qe.getQueryGraph();
		final Map<String,Integer> proximites = m_qe.getProximities();

		List<GTable<String>> resultTables = m_qe.getResultTables() == null ? new ArrayList<GTable<String>>() : m_qe.getResultTables(); 

		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
								
				int e1score = proximites.get(s1) * proximites.get(d1);
				int e2score = proximites.get(s2) * proximites.get(d2);
				
				if (e1score == e2score) {
					Integer ce1 = m_qe.getIndex().getObjectCardinality(e1.getLabel());
					Integer ce2 = m_qe.getIndex().getObjectCardinality(e2.getLabel());
					
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

		toVisit.addAll(m_qe.toVisit());
		

		Map<String,List<GraphEdge<QueryNode>>> prunedParts = new HashMap<String,List<GraphEdge<QueryNode>>>();
		if (m_doRefinement) {
			for (List<GraphEdge<QueryNode>> part : q.getPrunedParts()) {
				for (GraphEdge<QueryNode> edge : part) {
					String src = queryGraph.getSourceNode(edge).getName();
					String trg = queryGraph.getTargetNode(edge).getName();
	
					if (!q.getRemovedNodes().contains(src)) {
						if (prunedParts.containsKey(src))
							prunedParts.get(src).addAll(part);
						else
							prunedParts.put(src, part);
					}
					if (!q.getRemovedNodes().contains(trg)) {
						if (prunedParts.containsKey(trg))
							prunedParts.get(trg).addAll(part);
						else
							prunedParts.put(trg, part);
					}
					
				}
			}
		
			toVisit.removeAll(q.getPrunedEdges());
			counters.set(Counters.DM_REM_EDGES, q.getPrunedEdges().size());
			counters.set(Counters.DM_REM_NODES, q.getRemovedNodes().size());
		}
		
		Set<String> visited = new HashSet<String>();
		Set<String> verified = new HashSet<String>();
		
		while (toVisit.size() > 0) {
			String property, srcLabel, trgLabel;
			GraphEdge<QueryNode> currentEdge;
			
			List<GraphEdge<QueryNode>> skipped = new ArrayList<GraphEdge<QueryNode>>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
				trgLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
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
				result = joinWithTable(property, srcLabel, trgLabel, targetTable, Index.PO, targetTable.getColumn(trgLabel));
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				result = joinWithTable(property, srcLabel, trgLabel, sourceTable, Index.PS, sourceTable.getColumn(srcLabel));
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
		
		return m_qe.getResult().getRows();
	}

	private GTable<String> refineWithPrunedPart(List<GraphEdge<QueryNode>> part, String srcLabel, GTable<String> result) throws StorageException {
		Map<String,List<String[]>> ext2entity = new HashMap<String,List<String[]>>(100);
		GTable<String> extTable = new GTable<String>(Arrays.asList(srcLabel));
		
		int col = result.getColumn(srcLabel);
		
		Set<String> values = new HashSet<String>();
		for (String[] row : result) {
			String ext = m_es.getDataItem(IndexDescription.SES, row[col]);
			if (values.add(ext))
				extTable.addRow(new String[] { ext });
			
			List<String[]> entities = ext2entity.get(ext);
			if (entities == null) {
				entities = new ArrayList<String[]>(200);
				ext2entity.put(ext, entities);
			}
			entities.add(row);
		}
		
		m_matcher.setPrunedPart(part, srcLabel, extTable);
		m_matcher.match();
		
		int rows = result.rowCount();
		result = new GTable<String>(result, false);
		for (String ext : m_matcher.getValidExtensions()) {
			result.addRows(ext2entity.get(ext));
		}
		log.debug(" refined: " + rows + " => " + result.rowCount());
		
		return result;
	}
	
	private GTable<String> joinWithTable(String property, String srcLabel, String trgLabel, GTable<String> table, Index index, int col) throws StorageException, IOException {
		GTable<String> t2 = new GTable<String>(srcLabel, trgLabel);
		
		boolean targetConstant = Util.isConstant(trgLabel);
		Set<String> values = new HashSet<String>();
		for (String[] row : table) {
			if (values.add(row[col])) {
				 GTable<String> t3 = m_ls.getIndexTable(index, property, row[col]);
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
			GTable<String> table = m_ls.getIndexTable(Index.PO, property, trgLabel);
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
					t2.addRows(m_ls.getIndexTable(Index.PS, property, row[col]).getRows());
			}
			log.debug("unique values: " + values.size());
			table = Tables.mergeJoin(sourceTable, t2, Arrays.asList(srcLabel, trgLabel));
		}
		else {
			table = joinWithTable(property, srcLabel, trgLabel, sourceTable, Index.PS, sourceTable.getColumn(srcLabel));
			
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
