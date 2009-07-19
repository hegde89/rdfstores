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
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.sig.EvaluationClass;

public class QueryExecution {
	private IndexReader m_idxReader;
	private StructuredQuery m_query;
	private PrunedQuery m_prunedQuery;
	private QueryGraph m_queryGraph;
	private QueryGraph m_prunedQueryGraph;
	
	private List<QueryEdge> m_imVisitedEdges;
	private List<QueryEdge> m_imToVisit;
	
	private List<QueryEdge> m_visitedEdges;
	private List<QueryEdge> m_toVisit;
	
	// common
	private Map<String,Integer> m_proximities;
	private Map<String,String> m_removedProperties;
	
	// vp stuff
	private List<GTable<String>> m_resultTables = null;
	
	// index matcher stuff
	private List<GTable<String>> m_matchTables = null;
	private GTable<String> m_indexMatches = null;
	
	// index matches validator stuff
	private List<EvaluationClass> m_evaClasses = null;
	private Map<String,Integer> m_cardinalityMap = null;

	private GTable<String> m_result = null;
	
	// compaction signatures
	private Set<String> m_compactSigs;
	private int m_rowsBeforeCompaction = 0;
	
	private int m_status;
	
	private static final Logger log = Logger.getLogger(QueryExecution.class);
	
	public QueryExecution(StructuredQuery query, IndexReader index) {
		m_idxReader = index;
		m_query = query;
		m_queryGraph = query.getQueryGraph();
		m_proximities = query.calculateConstantProximities();
		m_toVisit = new ArrayList<QueryEdge>(m_queryGraph.edgeSet());
		m_visitedEdges = new ArrayList<QueryEdge>();
		m_imToVisit = new ArrayList<QueryEdge>(m_queryGraph.edgeSet());
		m_imVisitedEdges = new ArrayList<QueryEdge>();
		
		m_removedProperties = new HashMap<String,String>();
		
		m_compactSigs = new HashSet<String>();
		
		initialize();
	}
	
	private void initialize() {
		// TODO move this to Query class
//		for (QueryEdge e : m_queryGraph.edgeSet()) {
//			if (m_query.getRemovedNodes().contains(m_queryGraph.getNode(e.getSrc()).getSingleMember())
//				|| m_query.getRemovedNodes().contains(m_queryGraph.getNode(e.getDst()).getSingleMember())) 
//				m_removedProperties.put(m_queryGraph.getNode(e.getSrc()).getSingleMember(), e.getLabel());
//		}
	}
	
	
	public StructuredQuery getQuery() {
		return m_query;
	}
	
	public PrunedQuery getPrunedQuery() throws IOException {
		if (m_prunedQuery == null) {
			m_prunedQuery = new PrunedQuery(m_query, m_idxReader.getStructureIndex());
			m_prunedQueryGraph = m_prunedQuery.getQueryGraph();
		}
		
		return m_prunedQuery;
	}
	
	public QueryGraph getQueryGraph() {
		return m_queryGraph;
	}
	
	
	public List<QueryEdge> toVisit() {
		return m_toVisit;
	}
	
	public void setToVisit(List<QueryEdge> toVisit) {
		m_toVisit = toVisit;
	}
	
	public List<QueryEdge> getVisited() {
		return m_visitedEdges;
	}
	
	public void visited(QueryEdge edge) {
		m_toVisit.remove(edge);
		m_visitedEdges.add(edge);
	}
	
	// common -----------------------
	
	public Map<String,Integer> getProximities() {
		return m_proximities;
	}
	
	public Map<String,String> getRemovedProperties() {
		return m_removedProperties;
	}
	
	// vp ---------------------------
	
	public void setResultTables(List<GTable<String>> tables) {
		m_resultTables = tables;
	}
	
	public List<GTable<String>> getResultTables() {
		return m_resultTables;
	}
	
	// im ---------------------------
	
	public List<QueryEdge> imToVisit() {
		return m_imToVisit;
	}
	
	public List<QueryEdge> getIMVisited() {
		return m_imVisitedEdges;
	}
	
	public void imVisited(QueryEdge edge) {
		m_imToVisit.remove(edge);
		m_imVisitedEdges.add(edge);
	}
	
	public void setMatchTables(List<GTable<String>> tables) {
		m_matchTables = tables;
	}
	
	public List<GTable<String>> getMatchTables() {
		return m_matchTables;
	}
	
	public void setIndexMatches(GTable<String> indexMatches) {
		m_indexMatches = indexMatches;
	}
	
	public GTable<String> getIndexMatches() {
		return m_indexMatches;
	}
	
	// dm ---------------------------
	
	public void setEvaluationClasses(List<EvaluationClass> classes) {
		m_evaClasses = classes;
	}
	
	public List<EvaluationClass> getEvaluationClasses() {
		return m_evaClasses;
	}
	
	public void setCardinalityMap(Map<String,Integer> map) {
		m_cardinalityMap = map;
	}
	
	public Map<String,Integer> getCardinalityMap() {
		return m_cardinalityMap;
	}
	
	private GTable<String> compactTable(GTable<String> table) {
		m_rowsBeforeCompaction += table.rowCount();
		
		List<String> selectVars = m_query.getSelectVariableLabels();
		GTable<String> result = new GTable<String>(selectVars);

		int[] cols = new int [selectVars.size()];
		for (int i = 0; i < selectVars.size(); i++)
			cols[i] = table.getColumn(selectVars.get(i));
		
		for (String[] row : table) {
			String[] selectRow = new String [cols.length];
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < cols.length; i++) {
				selectRow[i] = row[cols[i]];
				sb.append(row[cols[i]]).append("__");
			}
			
			String sig = sb.toString();
			if (m_compactSigs.add(sig))
				result.addRow(selectRow);
		}
		
		return result;
	}
	
	public void addResult(GTable<String> result, boolean compact) {
		if (compact)
			result = compactTable(result);
		
		if (m_result == null)
			m_result = result;
		else
			m_result.addRows(result.getRows());
	}
	
	public GTable<String> getResult() {
		return m_result;
	}
	
	public void finished() {
		if (m_result != null)
			log.info("compaction: " + m_rowsBeforeCompaction + " => " + m_result.rowCount());
	}

	public QueryGraph getPrunedQueryGraph() throws IOException {
		return getPrunedQuery().getQueryGraph();
	}
} 
