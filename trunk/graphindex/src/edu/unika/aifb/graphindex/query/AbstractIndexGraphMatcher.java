package edu.unika.aifb.graphindex.query;

import java.util.HashMap;
import java.util.Set;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public abstract class AbstractIndexGraphMatcher implements IndexGraphMatcher {

	protected StructureIndex m_index;
	protected QueryExecution m_qe;
	protected String m_graphName;
	protected Query m_query;
	protected Graph<QueryNode> m_queryGraph;
	protected ExtensionStorage m_es;
	protected GraphStorage m_gs;
	protected HashMap<String,GTable<String>> m_p2to;
	protected HashMap<String,GTable<String>> m_p2ts;
	protected HashMap<String,Integer> m_inDegree;

	protected Timings m_timings;
	
	protected AbstractIndexGraphMatcher(StructureIndex index, String graphName) {
		m_index = index;
		m_graphName = graphName;
		m_es = index.getExtensionManager().getExtensionStorage();
		m_gs = index.getGraphManager().getGraphStorage();
		
		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("this index matcher is incompatible with the index");
	}
	
	public void initialize() throws StorageException {
		m_p2ts = new HashMap<String,GTable<String>>();
		m_p2to = new HashMap<String,GTable<String>>();
		
		m_inDegree = new HashMap<String,Integer>();
		
		Set<LabeledEdge<String>> edges = m_gs.loadEdges(m_graphName);
		
		for (LabeledEdge<String> e : edges) {

			GTable<String> table = m_p2ts.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2ts.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });

			table = m_p2to.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2to.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });
			
			if (!m_inDegree.containsKey(e.getSrc()))
				m_inDegree.put(e.getSrc(), 0);
			
			Integer deg = m_inDegree.get(e.getDst());
			if (deg == null)
				m_inDegree.put(e.getDst(), 1);
			else
				m_inDegree.put(e.getDst(), deg + 1);
		}
		
		for (GTable<String> t : m_p2ts.values())
			t.sort(0);
		for (GTable<String> t : m_p2to.values())
			t.sort(1);
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
		m_query = qe.getQuery();
		m_queryGraph = qe.getQueryGraph();
	}
	
	protected String getSourceLabel(GraphEdge<QueryNode> edge) {
		return m_queryGraph.getNode(edge.getSrc()).getName();
	}
	
	protected String getTargetLabel(GraphEdge<QueryNode> edge) {
		return m_queryGraph.getNode(edge.getDst()).getName();
	}

	protected abstract boolean isCompatibleWithIndex();

	public void setTimings(Timings timings) {
		m_timings = timings;
	}
}
