package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.DataField;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class VPEvaluator {
	private QueryExecution m_qe;
	private StructureIndex m_index;
	private ExtensionStorage m_es;
	
	private IndexDescription m_idxPSESO;
	private IndexDescription m_idxPOESS;
	private IndexDescription m_idxPOES;
	private IndexDescription m_idxPSES;

	private Timings m_timings;
	
	private static final Logger log = Logger.getLogger(VPEvaluator.class);
	
	public VPEvaluator() {
	}
	
	protected boolean isCompatibleWithIndex() {
		m_idxPSESO = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT, DataField.OBJECT);
		m_idxPOESS = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT, DataField.SUBJECT);
		m_idxPOES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		m_idxPSES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT);
		
		if (m_idxPSESO == null || m_idxPOESS == null || m_idxPOES == null || m_idxPSES == null)
			return false;
		
		return true;
	}

	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
		m_index = m_qe.getIndex();
		m_es = m_qe.getIndex().getExtensionManager().getExtensionStorage();
		m_timings = new Timings();
		
		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("incompatible index");
	}
	
	public void evaluate() throws StorageException {
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
//				return 0;
			}
		});
		toVisit.addAll(m_qe.toVisit());
		
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();

			String property = currentEdge.getLabel();
			String srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String trgLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			GTable<String> sourceTable = null, targetTable = null;
			for (GTable<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}
			
			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			log.debug("src table: " + sourceTable + ", trg table: " + targetTable);

			GTable<String> result;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				result = joinWithTable(property, srcLabel, trgLabel, targetTable, m_idxPOESS, targetTable.getColumn(trgLabel));
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				result = joinWithTable(property, srcLabel, trgLabel, sourceTable, m_idxPSESO, sourceTable.getColumn(srcLabel));
			}
			else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				result = evaluateBothUnmatched(property, srcLabel, trgLabel);
			}
			else {
				// case 3: both nodes already processed
				result = evaluateBothMatched(property, srcLabel, trgLabel, sourceTable, targetTable);
			}
			
			resultTables.add(result);
			m_qe.visited(currentEdge);
			
			log.debug("");
		}
		
		m_qe.addResult(resultTables.get(0), true);
	}
	
	private GTable<String> joinWithTable(String property, String srcLabel, String trgLabel, GTable<String> table, IndexDescription index, int col) throws StorageException {
		GTable<String> t2 = new GTable<String>(srcLabel, trgLabel);
		
		Set<String> values = new HashSet<String>();
		for (String[] row : table) {
			if (values.add(row[col]))
				t2.addRows(m_es.getIndexTable(srcLabel, trgLabel, index, property, row[col]).getRows());
		}
		log.debug("unique values: " + values.size());
		
		String joinCol = table.getColumnName(col);
		
		table.sort(joinCol, true);
		t2.sort(joinCol, true);

		GTable<String> result = Tables.mergeJoin(table, t2, joinCol);
		
		return result;
	}
	
	private GTable<String> evaluateBothUnmatched(String property, String srcLabel, String trgLabel) throws StorageException {
		if (Util.isConstant(trgLabel)) {
			return m_es.getIndexTable(srcLabel, trgLabel, m_idxPOESS, property, trgLabel);
		}
		else
			throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
	}
	
	private GTable<String> evaluateBothMatched(String property, String srcLabel, String trgLabel, GTable<String> sourceTable, GTable<String> targetTable) {
		// TODO Auto-generated method stub
		return null;
	}
}
