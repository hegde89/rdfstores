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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

/**
 * VPEvaluator evaluates structured queries using only the data index.
 * 
 * @author gla
 */
public class VPEvaluator extends StructuredQueryEvaluator {
	private QueryExecution m_qe;
	private DataIndex m_dataIndex;
	
	private IndexDescription m_idxPSO;
	private IndexDescription m_idxPOS;

	private Timings m_timings;
	
	private static final Logger log = Logger.getLogger(VPEvaluator.class);
	
	public VPEvaluator(IndexReader indexReader) throws IOException {
		super(indexReader);
		m_dataIndex = indexReader.getDataIndex();
		
		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("incomaptible index");
	}

	protected boolean isCompatibleWithIndex() {
		m_idxPSO = m_dataIndex.getSuitableIndex(DataField.PROPERTY, DataField.SUBJECT);
		m_idxPOS = m_dataIndex.getSuitableIndex(DataField.PROPERTY, DataField.OBJECT);
		
		log.debug("pso index: " + m_idxPSO + ", pos index: " + m_idxPOS);
		
		if (m_idxPSO == null || m_idxPOS == null)
			return false;
		
		return true;
	}

	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
		m_timings = new Timings();
		
		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("incompatible index");
	}
	
	@Override
	public Table<String> evaluate(StructuredQuery q) throws StorageException {
		if (m_qe == null) {
			m_qe = new QueryExecution(q, m_idxReader);
		}
		final QueryGraph queryGraph = q.getQueryGraph();//m_qe.getQueryGraph();
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
//				return 0;
			}
		});
		
		toVisit.addAll(m_qe.toVisit());
		
		while (toVisit.size() > 0) {
			QueryEdge currentEdge = toVisit.poll();

			String property = currentEdge.getLabel();
			String srcLabel = currentEdge.getSource().getLabel();
			String trgLabel = currentEdge.getTarget().getLabel();
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			Table<String> sourceTable = null, targetTable = null;
			for (Table<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}
			
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
			
			log.debug("res: " + result);
			
			resultTables.add(result);
			m_qe.visited(currentEdge);

			log.debug("");
		}

		m_qe.addResult(resultTables.get(0), true);
		
		return resultTables.get(0);
	}
	
	private Table<String> joinWithTable(String property, String srcLabel, String trgLabel, Table<String> table, IndexDescription index, DataField df, int col) throws StorageException {
		Table<String> t2 = new Table<String>(srcLabel, trgLabel);
		
		Set<String> values = new HashSet<String>();
		for (String[] row : table) {
			if (values.add(row[col])) {
				
				Table<String> t3 = m_dataIndex.getIndexStorage(index).getTable(index, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, index.createValueArray(DataField.PROPERTY, property, df, row[col]));
				if (Util.isConstant(trgLabel)) {
					for (String[] t3row : t3)
						if (t3row[1].equals(trgLabel))
							t2.addRow(t3row);
				}
				else
					t2.addRows(t3.getRows());
			}
		}
		log.debug("unique values: " + values.size());
		
		String joinCol = table.getColumnName(col);
		
		table.sort(joinCol, true);
		t2.sort(joinCol, true);

		Table<String> result = Tables.mergeJoin(table, t2, joinCol);
		
		return result;
	}
	
	private Table<String> evaluateBothUnmatched(String property, String srcLabel, String trgLabel) throws StorageException {
		if (Util.isConstant(trgLabel)) {
			Table<String> table = m_dataIndex.getIndexStorage(m_idxPOS).getTable(m_idxPOS, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, m_idxPOS.createValueArray(DataField.PROPERTY, property, DataField.OBJECT, trgLabel));
			table.setColumnName(0, srcLabel);
			table.setColumnName(1, trgLabel);
			return table;
		}
		else
			throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
	}
	
	private Table<String> evaluateBothMatched(String property, String srcLabel, String trgLabel, Table<String> sourceTable, Table<String> targetTable) throws StorageException {

		Table<String> table = joinWithTable(property, srcLabel, trgLabel, sourceTable, m_idxPSO, DataField.SUBJECT, sourceTable.getColumn(srcLabel));
		
		table.sort(trgLabel, true);
		targetTable.sort(trgLabel, true);
		
		table = Tables.mergeJoin(table, targetTable, trgLabel);
		
		return table;
	}

}
