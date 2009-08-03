package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.Cursor;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.GraphElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.NodeElement;

public class StructuredMatchElement extends GraphElement {
	private Table<String> m_table, m_extTable;
	private Set<NodeElement> m_nodes;
	private HybridQuery m_query;
	
	public StructuredMatchElement(String label, HybridQuery query, Set<NodeElement> nodes, Table<String> table, Table<String> extTable) {
		super(label);
		m_nodes = nodes;
		m_table = table;
		m_extTable = extTable;
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
	
	private NodeElement getNode(String label) {
		for (String[] row : m_extTable)
			for (NodeElement node : m_nodes)
				if (node.getLabel().equals(row[m_table.getColumn(label)]))
					return node;
		return null;
	}
	
	public Set<EdgeElement> getQueryEdges() {
		Set<EdgeElement> edges = new HashSet<EdgeElement>();
		for (QueryEdge queryEdge : m_query.getStructuredQuery().getQueryGraph().edgeSet()) {
			NodeElement src = getNode(queryEdge.getSource().getLabel());
			NodeElement trg = getNode(queryEdge.getTarget().getLabel());
			if (trg == null)
				trg = new NodeElement(queryEdge.getTarget().getLabel());
			EdgeElement e = new EdgeElement(src, queryEdge.getLabel(), trg);
			edges.add(e);
		}
		return edges;
	}

	@Override
	public List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor) {
		Set<GraphElement> neighbors = new HashSet<GraphElement>();
		
		for (NodeElement node : m_nodes)
			neighbors.addAll(node.getNeighbors(graph, cursor));
		
		return new ArrayList<GraphElement>(neighbors);
	}

}
