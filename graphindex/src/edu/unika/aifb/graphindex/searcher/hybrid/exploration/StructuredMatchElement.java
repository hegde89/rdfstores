package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.Cursor;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.GraphElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.NodeElement;

public class StructuredMatchElement extends GraphElement {
	private GTable<String> m_table;
	private Set<NodeElement> m_nodes;
	private HybridQuery m_query;
	
	public StructuredMatchElement(String label, HybridQuery query, Set<NodeElement> nodes, GTable<String> table) {
		super(label);
		m_nodes = nodes;
		m_table = table;
		m_query = query;
	}
	
	public Set<NodeElement> getNodes() {
		return m_nodes;
	}
	
	public void setNodes(Set<NodeElement> nodes) {
		m_nodes = nodes;
	}
	
	public HybridQuery getQuery() {
		return m_query;
	}

	@Override
	public List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor) {
		Set<GraphElement> neighbors = new HashSet<GraphElement>();
		
		for (NodeElement node : m_nodes)
			neighbors.addAll(node.getNeighbors(graph, cursor));
		
		return new ArrayList<GraphElement>(neighbors);
	}

}
