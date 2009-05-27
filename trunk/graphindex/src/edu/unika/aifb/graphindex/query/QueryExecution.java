package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;

public class QueryExecution {
	private StructureIndex m_index;
	private Query m_query;
	private Graph<QueryNode> m_queryGraph;
	
	private List<GraphEdge<QueryNode>> m_imVisitedEdges;
	private List<GraphEdge<QueryNode>> m_imToVisit;
	
	private List<GraphEdge<QueryNode>> m_visitedEdges;
	private List<GraphEdge<QueryNode>> m_toVisit;
	
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
	
	public QueryExecution(Query query, StructureIndex index) {
		m_index = index;
		m_query = query;
		m_queryGraph = query.getGraph();
		m_proximities = query.calculateConstantProximities();
		m_toVisit = m_queryGraph.edges();
		m_visitedEdges = new ArrayList<GraphEdge<QueryNode>>();
		m_imToVisit = m_queryGraph.edges();
		m_imVisitedEdges = new ArrayList<GraphEdge<QueryNode>>();
		
		m_removedProperties = new HashMap<String,String>();
		
		m_compactSigs = new HashSet<String>();
		
		initialize();
	}
	
	private void initialize() {
		// TODO move this to Query class
		for (GraphEdge<QueryNode> e : m_queryGraph.edges()) {
			if (m_query.getRemovedNodes().contains(m_queryGraph.getNode(e.getSrc()).getSingleMember())
				|| m_query.getRemovedNodes().contains(m_queryGraph.getNode(e.getDst()).getSingleMember())) 
				m_removedProperties.put(m_queryGraph.getNode(e.getSrc()).getSingleMember(), e.getLabel());
		}
	}
	
	
	public StructureIndex getIndex() {
		return m_index;
	}
	
	public Query getQuery() {
		return m_query;
	}
	
	public Graph<QueryNode> getQueryGraph() {
		return m_queryGraph;
	}
	
	
	public List<GraphEdge<QueryNode>> toVisit() {
		return m_toVisit;
	}
	
	public void setToVisit(List<GraphEdge<QueryNode>> toVisit) {
		m_toVisit = toVisit;
	}
	
	public List<GraphEdge<QueryNode>> getVisited() {
		return m_visitedEdges;
	}
	
	public void visited(GraphEdge<QueryNode> edge) {
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
	
	public List<GraphEdge<QueryNode>> imToVisit() {
		return m_imToVisit;
	}
	
	public List<GraphEdge<QueryNode>> getIMVisited() {
		return m_imVisitedEdges;
	}
	
	public void imVisited(GraphEdge<QueryNode> edge) {
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
		
		GTable<String> result = new GTable<String>(m_query.getSelectVariables());

		int[] cols = new int [m_query.getSelectVariables().size()];
		for (int i = 0; i < m_query.getSelectVariables().size(); i++)
			cols[i] = table.getColumn(m_query.getSelectVariables().get(i));
		
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
		log.info("compaction: " + m_rowsBeforeCompaction + " => " + m_result.rowCount());
	}
} 
