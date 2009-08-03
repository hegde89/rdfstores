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

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.searcher.structured.sig.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

public class PrunedPartMatcher extends AbstractIndexGraphMatcher {

	private PrunedQueryPart m_part;
	private String m_startNode;
	private Table<String> m_startNodeTable;
	private Set<String> m_validExtensions;
	
	private static final Logger log = Logger.getLogger(PrunedPartMatcher.class);

	protected PrunedPartMatcher(IndexReader idxReader) throws IOException {
		super(idxReader);

	}

	@Override
	public boolean isCompatibleWithIndex() {
		return true;
	}

	public void setPrunedPart(PrunedQueryPart part, String startNode, Table<String> startNodeTable) {
		m_part = part;
		m_startNode = startNode;
		m_startNodeTable = startNodeTable;
		m_validExtensions = new HashSet<String>(200);
	}
	
	public Set<String> getValidExtensions() {
		return m_validExtensions;
	}
	
	public void match() throws StorageException {
		final QueryGraph queryGraph = m_part.getQueryGraph();
		final Map<String,Integer> proximities = m_qe.getProximities();
		
		Queue<QueryEdge> toVisit = new PriorityQueue<QueryEdge>(queryGraph.edgeCount(), new Comparator<QueryEdge>() {
			public int compare(QueryEdge e1, QueryEdge e2) {
				String s1 = e1.getSource().getLabel();
				String s2 = e2.getSource().getLabel();
				String d1 = e1.getTarget().getLabel();
				String d2 = e2.getTarget().getLabel();
								
				int e1score = proximities.get(s1) * proximities.get(d1);
				int e2score = proximities.get(s2) * proximities.get(d2);
				
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
		
		toVisit.addAll(m_part.getQueryGraph().edgeSet());
		Set<String> visited = new HashSet<String>();
		visited.add(m_startNode);
		
		List<Table<String>> resultTables = new ArrayList<Table<String>>();
		resultTables.add(m_startNodeTable);
		
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
			
			log.debug(" " + srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			Table<String> sourceTable = null, targetTable = null, result;
			for (Table<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}

			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			log.debug(" src table: " + sourceTable + ", trg table: " + targetTable);
			
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				Table<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);

				targetTable.sort(trgLabel, true);
				result = Tables.mergeJoin(targetTable, edgeTable, trgLabel);
			} else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				Table<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);

				sourceTable.sort(srcLabel, true);
				result = Tables.mergeJoin(sourceTable, edgeTable, srcLabel);
			} else if (sourceTable != null && targetTable != null){
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
					result = Tables.mergeJoin(first, edgeTable, firstCol);
	
					result.sort(secondCol, true);
					second.sort(secondCol, true);
					result = Tables.mergeJoin(second, result, secondCol);
				}
			}
			else {
				log.error("what");
				result = null;
			}
		
			if (result == null || result.rowCount() == 0) {
				resultTables.clear();
				break;
			}
			
			resultTables.add(result);
		}

		for (Table<String> resultTable : resultTables) {
			if (resultTable.hasColumn(m_startNode)) {
				int col = resultTable.getColumn(m_startNode);
				for (String[] row : resultTable)
					m_validExtensions.add(row[col]);
				break;
			}
		}
	}
	
	private Table<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy) throws StorageException {
		Table<String> edgeTable;
		if (orderedBy == 0)
			edgeTable = m_p2ts.get(property);
		else
			edgeTable = m_p2to.get(property);

		if (edgeTable == null) {
			throw new UnsupportedOperationException("error");
		}

		Table<String> table = new Table<String>(srcLabel, trgLabel);
		table.setRows(edgeTable.getRows());
		table.setSortedColumn(orderedBy);

		log.debug(" edge table rows: " + table.rowCount());
		return table;
	}

}
