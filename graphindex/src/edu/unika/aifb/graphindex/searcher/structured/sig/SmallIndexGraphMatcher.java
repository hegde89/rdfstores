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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Tables.JoinedRowValidator;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class SmallIndexGraphMatcher extends AbstractIndexGraphMatcher {
	private class SignatureRowValidator implements JoinedRowValidator {
		private Set<String> signatures;
		private List<Integer> leftSigCols, rightSigCols;
		private boolean nopurge = true;
		public int purged = 0, total = 0;

		public boolean alwaysPurge = false;

		public void setTables(Table<String> left, Table<String> right) {
			leftSigCols = getSignatureColumns(left);
			rightSigCols = getSignatureColumns(right);

//			log.debug("last purge: " + purged + "/" + total);
//			log.debug(left + " " + leftSigCols);
//			log.debug(right + " " + rightSigCols);

			purged = 0;
			total = 0;

			if (!alwaysPurge && (leftSigCols.size() + rightSigCols.size() == 0 || (leftSigCols.size() == left.columnCount() && rightSigCols.size() == right.columnCount()))){// || left.rowCount() > 5000 || right.rowCount() > 5000)) {
				nopurge = true;
				log.debug("no purge");
				return;
			}

			signatures = new HashSet<String>((left.rowCount() + right.rowCount()) / 2);
			nopurge = false;
		}

		public boolean isValid(String[] leftRow, String[] rightRow) {
			if (nopurge || !m_purgeNeeded)
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
	
	private IndexStorage m_is;
	private SignatureRowValidator m_validator;
	private final Set<String> m_signatureNodes, m_joinNodes;
	private final Map<String,Integer> m_joinCounts;
	private boolean m_purgeNeeded;

	private IndexDescription m_idxPSESO;
	private IndexDescription m_idxPOESS;
	private IndexDescription m_idxPOES;
	private IndexDescription m_idxSES;

	private static final Logger log = Logger.getLogger(SmallIndexGraphMatcher.class);

	public SmallIndexGraphMatcher(IndexReader idxReader) throws IOException {
		super(idxReader);
		m_signatureNodes = new HashSet<String>();
		m_joinNodes = new HashSet<String>();
		m_joinCounts = new HashMap<String,Integer>();
		m_validator = new SignatureRowValidator();
		
		m_is = idxReader.getStructureIndex().getSPIndexStorage();
	}

	@Override
	protected boolean isCompatibleWithIndex() throws IOException {
		m_idxPSESO = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT, DataField.OBJECT);
		m_idxPOESS = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT, DataField.SUBJECT);
		m_idxPOES = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		m_idxSES = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.SUBJECT, DataField.EXT_SUBJECT);

		if (m_idxPSESO == null || m_idxPOESS == null || m_idxPOES == null || m_idxSES == null)
			return false;

		return true;
	}

	public void match() throws StorageException {

		final Map<String,Integer> scores = m_query.calculateConstantProximities();
		for (String label : scores.keySet())
			if (scores.get(label) > 1)
				scores.put(label, 10);

		log.debug("constant proximities: " + scores);

		PriorityQueue<QueryEdge> toVisit = new PriorityQueue<QueryEdge>(m_queryGraph.edgeCount(), new Comparator<QueryEdge>() {
			public int compare(QueryEdge e1, QueryEdge e2) {
				String s1 = e1.getSource().getLabel();
				String s2 = e2.getSource().getLabel();
				String d1 = e1.getTarget().getLabel();
				String d2 = e2.getTarget().getLabel();

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
					Table<String> t1 = m_p2to.get(e1.getLabel());
					Table<String> t2 = m_p2to.get(e2.getLabel());

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
				} else if (g1)
					return -1;
				else
					return 1;
			}
		});

		List<Table<String>> resultTables = new ArrayList<Table<String>>();
		Set<String> visited = new HashSet<String>();

		if (m_qe.getIMVisited().size() > 0) {
			// probably from ASM
			for (QueryEdge edge : m_qe.getIMVisited()) {
				visited.add(edge.getSource().getLabel());
				visited.add(edge.getTarget().getLabel());
			}
			toVisit.addAll(m_qe.imToVisit());
			resultTables.addAll(m_qe.getMatchTables());
		} else
			toVisit.addAll(m_queryGraph.edgeSet());

		m_counters.set(Counters.IM_PROCESSED_EDGES, toVisit.size());

		while (toVisit.size() > 0) {
			QueryEdge currentEdge;
			String srcLabel, trgLabel, property;

			// start with query edge with at least one constant, after that, only use
			// edges connected to an already processed edge. never evaluate lone edges
			// with both positions being variables
			List<QueryEdge> skipped = new ArrayList<QueryEdge>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcLabel = currentEdge.getSource().getLabel();
				trgLabel = currentEdge.getTarget().getLabel();
			} while (!visited.contains(srcLabel) && !visited.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel));

			skipped.remove(currentEdge);
			toVisit.addAll(skipped);

			visited.add(srcLabel);
			visited.add(trgLabel);

			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");

			Table<String> sourceTable = null, targetTable = null, result;
			for (Table<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}

			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			log.debug("src table: " + sourceTable + ", trg table: " + targetTable);

//			if (toVisit.size() == 0)
//				m_purgeNeeded = true;
//			if (toVisit.size() == 0) {
//				m_validator = new SignatureRowValidator();
//			}
//			else
//				m_validator = null;
//			m_validator.alwaysPurge = true;
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				Table<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);

				targetTable.sort(trgLabel, true);
				result = Tables.mergeJoin(targetTable, edgeTable, trgLabel, m_validator);
			} else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				Table<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0, sourceTable);

				sourceTable.sort(srcLabel, true);
				result = Tables.mergeJoin(sourceTable, edgeTable, srcLabel, m_validator);
			} else if (sourceTable == null && targetTable == null) {
				// case 2: edge has two unprocessed nodes
				Table<String> edgeTable;
				if (Util.isConstant(srcLabel) || Util.isConstant(trgLabel)) {
					int is = findNextIntersection(currentEdge, toVisit);
					if (is == QueryEdge.IS_SRC || is == QueryEdge.IS_SRCDST)
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);
					else
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);
				} else {
					throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
				}

				result = edgeTable;
			} else {
				// case 3: both nodes already processed
				if (sourceTable == targetTable) {
					result = Tables.mergeJoin(sourceTable, getEdgeTable(property, srcLabel, trgLabel, 0), Arrays.asList(srcLabel, trgLabel));
				}
				else {
					Table<String> first, second;
					String firstCol, secondCol;
					Table<String> edgeTable;
	
					// start with smaller table
					if (sourceTable.rowCount() < targetTable.rowCount()) {
						first = sourceTable;
						firstCol = srcLabel;
	
						second = targetTable;
						secondCol = trgLabel;
	
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);
					} else {
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
			}

			if (result.rowCount() == 0) {
				m_qe.setIndexMatches(null);
				return;
			}

			processed(srcLabel, trgLabel);

			resultTables.add(result);

			m_counters.set(Counters.IM_INDEX_MATCHES, result.rowCount());

			log.debug("tables: " + resultTables);
			if (m_validator != null)
				log.debug("purged: " + m_validator.purged + "/" + m_validator.total);
			log.debug(m_joinCounts);
			log.debug("");
		}

		if (resultTables.size() == 1)
			m_qe.setIndexMatches(resultTables.get(0));
		else
			m_qe.setIndexMatches(null);
//		log.debug(m_qe.getIndexMatches().toDataString());
	}

	private Table<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy) throws StorageException {
		return getEdgeTable(property, srcLabel, trgLabel, orderedBy, null);
	}

	private Map<String,String> m_inventedExtensions = new HashMap<String,String>();
	private int m_extCounter = 0;

	private Table<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy, Table<String> sourceTable) throws StorageException {
		Table<String> edgeTable;
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
				List<String> subjectExtensions = m_is.getDataList(m_idxPOES, DataField.EXT_SUBJECT, m_idxPOES.createValueArray(DataField.PROPERTY, property, DataField.OBJECT, trgLabel));
				log.debug("subject extensions: " + subjectExtensions.size());
				edgeTable = new Table<String>(srcLabel, trgLabel);
				for (String subjectExt : subjectExtensions)
					edgeTable.addRow(new String[] { subjectExt, inventedExtension });
				edgeTable.sort(orderedBy);
//			} else if (sourceTable != null) {
//				edgeTable = new GTable<String>(srcLabel, trgLabel);
//				doTrgFilter = false;
//				int sourceCol = sourceTable.getColumn(srcLabel);
//				for (String[] srcRow : sourceTable) {
//					if (m_is.hasValues(m_idxPSESO, srcRow[sourceCol], property))
//						edgeTable.addRow(new String[] { srcRow[sourceCol], inventedExtension });
//				}
//				edgeTable.sort(orderedBy);
			} else
				throw new UnsupportedOperationException("error");
		}
		
		edgeTable.setColumnName(0, srcLabel);
		edgeTable.setColumnName(1, trgLabel);

		Table<String> table = new Table<String>(srcLabel, trgLabel);
		table.setRows(edgeTable.getRows());
		table.setSortedColumn(orderedBy);

		if (doTrgFilter && Util.isConstant(trgLabel)) {
			List<String[]> filtered = new ArrayList<String[]>();
			for (String[] row : table) {
				if (m_is.hasValues(m_idxPOESS, property, trgLabel, row[0]))
					filtered.add(row);
			}
			table.setRows(filtered);
		}

		log.debug("edge table rows: " + table.rowCount());
		return table;
	}

	private int findNextIntersection(QueryEdge edge, PriorityQueue<QueryEdge> nextEdges) {
		for (QueryEdge e : nextEdges) {
			int is = edge.intersect(e);
			if (is != QueryEdge.IS_NONE)
				return is;
		}
		return QueryEdge.IS_NONE;
	}

	@Override
	public void setQueryExecution(QueryExecution qe) {
		super.setQueryExecution(qe);

		m_signatureNodes.clear();
		m_joinNodes.clear();
		m_joinCounts.clear();

		for (QNode node : m_queryGraph.vertexSet()) {
			if (m_queryGraph.outDegreeOf(node) > 0 || node.isConstant())
				m_signatureNodes.add(node.getLabel());
			else if (m_queryGraph.inDegreeOf(node) > 1)
				m_joinNodes.add(node.getLabel());

			m_joinCounts.put(node.getLabel(), m_queryGraph.inDegreeOf(node) + m_queryGraph.outDegreeOf(node));
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

	private List<Integer> getSignatureColumns(Table<String> table) {
		List<Integer> sigCols = new ArrayList<Integer>();

		for (String colName : table.getColumnNames())
			if (m_signatureNodes.contains(colName) || (m_joinNodes.contains(colName) && m_joinCounts.get(colName) > 0))
				sigCols.add(table.getColumn(colName));

		return sigCols;
	}
}
