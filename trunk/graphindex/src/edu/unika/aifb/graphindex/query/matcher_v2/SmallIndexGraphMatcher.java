package edu.unika.aifb.graphindex.query.matcher_v2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sun.tools.extcheck.ExtCheck;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Tables.JoinedRowValidator;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.QueryExecution;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.DataField;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class SmallIndexGraphMatcher extends AbstractIndexGraphMatcher {
	private class SignatureRowValidator implements JoinedRowValidator {
		private Set<String> signatures;
		private List<Integer> leftSigCols, rightSigCols;
		private boolean nopurge = true;
		public int purged = 0, total = 0;
		
		public boolean alwaysPurge = false;
		
		public void setTables(GTable<String> left, GTable<String> right) {
			leftSigCols = getSignatureColumns(left);
			rightSigCols = getSignatureColumns(right);
			
//			log.debug("last purge: " + purged + "/" + total);
//			log.debug(left + " " + leftSigCols);
//			log.debug(right + " " + rightSigCols);

			purged = 0;
			total = 0;
			
			if (!alwaysPurge && (leftSigCols.size() + rightSigCols.size() == 0 
				|| (leftSigCols.size() == left.columnCount() && rightSigCols.size() == right.columnCount())
				|| left.rowCount() > 5000 || right.rowCount() > 5000)) {
				nopurge = true;
				log.debug("no purge");
				return;
			}
			
			signatures = new HashSet<String>((left.rowCount() + right.rowCount()) / 2);
			nopurge = false;
		}
		
		public boolean isValid(String[] leftRow, String[] rightRow) {
			if (nopurge)
				return true;
			
			m_timings.start(Timings.IM_PURGE);
			total++;
			
			StringBuffer sb = new StringBuffer();
			sb.append(getSignature(leftRow, leftSigCols)).append("||").append(getSignature(rightRow, rightSigCols));
			
			if (signatures.add(sb.toString())) {
				m_timings.end(Timings.IM_PURGE);
				return true;
			}
			
			purged++;
			
			m_timings.end(Timings.IM_PURGE);
			return false;
		}
	}
	
	private SignatureRowValidator m_validator;
	private final Set<String> m_signatureNodes, m_joinNodes;
	private final Map<String,Integer> m_joinCounts;
	private boolean m_purgeNeeded;
	
	private IndexDescription m_idxPSESO;
	private IndexDescription m_idxPOESS;
	private IndexDescription m_idxPOES;
	private IndexDescription m_idxPSES;

	private static final Logger log = Logger.getLogger(SmallIndexGraphMatcher.class);
	
	public SmallIndexGraphMatcher(StructureIndex index, String graphName) {
		super(index, graphName);
		m_signatureNodes = new HashSet<String>();
		m_joinNodes = new HashSet<String>();
		m_joinCounts = new HashMap<String,Integer>();
		m_validator = new SignatureRowValidator();
	}

	@Override
	protected boolean isCompatibleWithIndex() {
		m_idxPSESO = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT, DataField.OBJECT);
		m_idxPOESS = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT, DataField.SUBJECT);
		m_idxPOES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		m_idxPSES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT);
		
		if (m_idxPSESO == null || m_idxPOESS == null || m_idxPOES == null || m_idxPSES == null)
			return false;
		
		return true;
	}

	public void match() throws StorageException {
		
		final Map<String,Integer> scores = m_query.calculateConstantProximities();
		for (String label : scores.keySet())
			if (scores.get(label) > 1)
				scores.put(label, 10);
		
		log.debug("constant proximities: " + scores);

		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(m_queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = m_queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = m_queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = m_queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = m_queryGraph.getNode(e2.getDst()).getSingleMember();
			
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);
				
				// order first by proximity to a constant
				if (e1score < e2score)
					return -1;
				else if (e1score > e2score)
					return 1;
				
				// if the proximity is equal, prefer edges with a constant over edges without one 
				// TODO probably stupid: if both edges have the same proximity value, both either
				// have a constant or both don't
				boolean g1 = !s1.startsWith("?") || !d1.startsWith("?");
				boolean g2 = !s2.startsWith("?") || !d2.startsWith("?");
				
				if ((g1 && g2) || (!g1 && !g2)) {
					// if both or neither have constants, prefer the edge with smaller edge table
					GTable<String> t1 = m_p2to.get(e1.getLabel());
					GTable<String> t2 = m_p2to.get(e2.getLabel());
					
					if (t1 == null || t2 == null)
						return 0;
					
					int r1 = t1.rowCount();
					int r2 = t2.rowCount();
					
					if (r1 < r2)
						return -1;
					else if (r1 > r2)
						return 1;
					else 
						return 0;
				}
				else if (g1)
					return -1;
				else
					return 1;
			}
		});
		
		List<GTable<String>> resultTables = new ArrayList<GTable<String>>();
		Set<String> visited = new HashSet<String>();
		
		if (m_qe.getIMVisited().size() > 0) {
			// probably from ASM
			for (GraphEdge<QueryNode> edge : m_qe.getIMVisited()) {
				visited.add(getSourceLabel(edge));
				visited.add(getTargetLabel(edge));
			}
			toVisit.addAll(m_qe.imToVisit());
			resultTables.addAll(m_qe.getMatchTables());
		}
		else 
			toVisit.addAll(m_queryGraph.edges());

		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge;
			String srcLabel, trgLabel, property;
			
			// start with query edge with at least one constant, after that, only use
			// edges connected to an already processed edge. never evaluate lone edges
			// with both positions being variables
			List<GraphEdge<QueryNode>> skipped = new ArrayList<GraphEdge<QueryNode>>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcLabel = getSourceLabel(currentEdge);
				trgLabel = getTargetLabel(currentEdge);
			}
			while (!visited.contains(srcLabel) && !visited.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel));
			
			skipped.remove(currentEdge);
			toVisit.addAll(skipped);

			visited.add(srcLabel);
			visited.add(trgLabel);
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			GTable<String> sourceTable = null, targetTable = null, result;
			for (GTable<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}
			
			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);
			
			log.debug("src table: " + sourceTable + ", trg table: " + targetTable);
			
			processed(srcLabel, trgLabel);
			
//			if (toVisit.size() == 0) {
//				m_validator = new SignatureRowValidator();
//			}
//			else
//				m_validator = null;
			m_validator.alwaysPurge = true;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				GTable<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);
				
				targetTable.sort(trgLabel, true);
				result = Tables.mergeJoin(targetTable, edgeTable, trgLabel, m_validator);
			}
			else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				GTable<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0, sourceTable);
				
				sourceTable.sort(srcLabel, true);
				result = Tables.mergeJoin(sourceTable, edgeTable, srcLabel, m_validator);
			}
			else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				GTable<String> edgeTable;
				if (Util.isConstant(srcLabel) || Util.isConstant(trgLabel)) {
					int is = findNextIntersection(currentEdge, toVisit); 
					if (is == GraphEdge.IS_SRC || is == GraphEdge.IS_SRCDST)
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);
					else
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);
				}
				else {
					throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
				}
				
				result = edgeTable;
			}
			else {
				// case 3: both nodes already processed
				GTable<String> first, second;
				String firstCol, secondCol;
				GTable<String> edgeTable;
				
				// start with smaller table
				if (sourceTable.rowCount() < targetTable.rowCount()) {
					first = sourceTable;
					firstCol = srcLabel;
					
					second = targetTable;
					secondCol = trgLabel;

					edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);
				}
				else {
					first = targetTable;
					firstCol = trgLabel;
					
					second = sourceTable;
					secondCol = srcLabel;

					edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);
				}
				
				first.sort(firstCol, true);
				result = Tables.mergeJoin(first, edgeTable, firstCol, m_validator);
	
				result.sort(secondCol, true);
				second.sort(secondCol, true);
				result = Tables.mergeJoin(second, result, secondCol, m_validator);
			}
			
			log.debug("rows: " + result.rowCount());
			
			if (result.rowCount() == 0) {
				m_qe.setIndexMatches(null);
				return;
			}
			
			resultTables.add(result);
			
			log.debug("tables: " + resultTables);
			if (m_validator != null)
				log.debug("purged: " + m_validator.purged + "/" + m_validator.total);
			log.debug("");
		}
		
		if (resultTables.size() == 1)
			m_qe.setIndexMatches(resultTables.get(0));
		else
			m_qe.setIndexMatches(null);
//		log.debug(m_qe.getIndexMatches().toDataString());
	}
	
	private GTable<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy) throws StorageException {
		return getEdgeTable(property, srcLabel, trgLabel, orderedBy, null);
	}
	
	private Map<String,String> m_inventedExtensions = new HashMap<String,String>();
	private int m_extCounter = 0;
	
	private GTable<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy, GTable<String> sourceTable) throws StorageException {
		GTable<String> edgeTable; 
		if (orderedBy == 0)
			edgeTable = m_p2ts.get(property);
		else 
			edgeTable = m_p2to.get(property);
		
		boolean doTrgFilter = true;
		if (edgeTable == null) {
			String inventedExtension = m_inventedExtensions.get(trgLabel);
			if (inventedExtension == null) {
				inventedExtension = "bx" + m_extCounter++;
				m_inventedExtensions.put(trgLabel, inventedExtension);
			}
			if (Util.isConstant(trgLabel)) {
				doTrgFilter = false;
				List<String> subjectExtensions = m_es.getData(m_idxPOES, property, trgLabel);
				log.debug("subject extensions: " + subjectExtensions.size());
				edgeTable = new GTable<String>(srcLabel, trgLabel);
				for (String subjectExt : subjectExtensions)
					edgeTable.addRow(new String[] { subjectExt, inventedExtension });
				edgeTable.sort(orderedBy);
			}
			else if (sourceTable != null) {
				edgeTable = new GTable<String>(srcLabel, trgLabel);
				doTrgFilter = false;
				int sourceCol = sourceTable.getColumn(srcLabel);
				for (String[] srcRow : sourceTable) {
					if (m_es.hasTriples(m_idxPSESO, srcRow[sourceCol], property, null))
						edgeTable.addRow(new String[] { srcRow[sourceCol], inventedExtension });
				}
				edgeTable.sort(orderedBy);
			}
			else
				throw new UnsupportedOperationException("error");
		}
		
		GTable<String> table = new GTable<String>(srcLabel, trgLabel);
		table.setRows(edgeTable.getRows());
		table.setSortedColumn(orderedBy);
		
		if (doTrgFilter && Util.isConstant(trgLabel)) {
			List<String[]> filtered = new ArrayList<String[]>();
			for (String[] row : table) {
				if (m_es.hasTriples(m_idxPOESS, row[0], property, trgLabel))
					filtered.add(row);
			}
			table.setRows(filtered);
		}
		
		log.debug("edge table rows: " + table.rowCount());
		return table;
	}
	
	private int findNextIntersection(GraphEdge<QueryNode> edge, PriorityQueue<GraphEdge<QueryNode>> nextEdges) {
		for (GraphEdge<QueryNode> e : nextEdges) {
			int is = edge.intersect(e);
			if (is != GraphEdge.IS_NONE)
				return is;
		}
		return GraphEdge.IS_NONE;
	}
	
	@Override
	public void setQueryExecution(QueryExecution qe) {
		super.setQueryExecution(qe);
		
		m_signatureNodes.clear();
		m_joinNodes.clear();
		m_joinCounts.clear();
		
		for (int i = 0; i < m_queryGraph.nodeCount(); i++) {
			if (m_queryGraph.outDegreeOf(i) > 0 || Util.isConstant(m_queryGraph.getNode(i).getSingleMember()))
				m_signatureNodes.add(m_queryGraph.getNode(i).getSingleMember());
			else if (m_queryGraph.inDegreeOf(i) > 1)
				m_joinNodes.add(m_queryGraph.getNode(i).getSingleMember());
			
			m_joinCounts.put(m_queryGraph.getNode(i).getSingleMember(), m_queryGraph.inDegreeOf(i) + m_queryGraph.outDegreeOf(i));
		}
		
//		if (query.getRemovedNodes().size() > 0) {
//			for (String node : query.getRemovedNodes()) {
//				if (!m_signatureNodes.contains(node))
//					m_signatureNodes.remove(node);
//			}
//		}
		
		log.debug("sig nodes: " + m_signatureNodes);
		log.debug("join nodes: " + m_joinNodes);
	}

	private void processed(String n1, String n2) {
		m_purgeNeeded = false;
		
		int c = m_joinCounts.get(n1) - 1;
		m_joinCounts.put(n1, c);
		if (c == 0 && m_joinNodes.contains(n1))
			m_purgeNeeded = true;
		
		c = m_joinCounts.get(n2) - 1;
		m_joinCounts.put(n2, c);
		if (c == 0 && m_joinNodes.contains(n2))
			m_purgeNeeded = true;

		log.debug("purge needed: " + m_purgeNeeded);
	}

	private String getSignature(String[] row, List<Integer> sigCols) {
		StringBuilder sb = new StringBuilder();
		for (int sigCol : sigCols)
			sb.append(row[sigCol]).append("__");
		return sb.toString();
	}
	
	private List<Integer> getSignatureColumns(GTable<String> table) {
		List<Integer> sigCols = new ArrayList<Integer>();

		for (String colName : table.getColumnNames())
			if (m_signatureNodes.contains(colName) || (m_joinNodes.contains(colName) && m_joinCounts.get(colName) > 0)) 
				sigCols.add(table.getColumn(colName));

		return sigCols;
	}
}
