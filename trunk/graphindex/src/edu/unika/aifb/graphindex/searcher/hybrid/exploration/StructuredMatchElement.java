package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;

public class StructuredMatchElement extends GraphElement {
	private NodeElement m_node;
	private StructuredQuery m_query;
	
	public StructuredMatchElement(String label, StructuredQuery query, NodeElement node) {
		super(label);
		m_node = node;
		m_query = query;
	}
	
	public NodeElement getNode() {
		return m_node;
	}
	
	public void setNode(NodeElement node) {
		m_node = node;
	}
	
	public StructuredQuery getQuery() {
		return m_query;
	}
	
	public double getCost() {
		return 0;
//		return m_query.getQueryGraph().edgeSet().size();
	}
	
//	private NodeElement getNode(String label) {
//		for (String[] row : m_extTable)
//			for (NodeElement node : m_nodes)
//				if (node.getLabel().equals(row[m_table.getColumn(label)]))
//					return node;
//		return null;
//	}
//	
//	public Set<EdgeElement> getQueryEdges() {
//		Set<EdgeElement> edges = new HashSet<EdgeElement>();
//		for (QueryEdge queryEdge : m_query.getQueryGraph().edgeSet()) {
//			NodeElement src = getNode(queryEdge.getSource().getLabel());
//			NodeElement trg = getNode(queryEdge.getTarget().getLabel());
//			if (trg == null)
//				trg = new NodeElement(queryEdge.getTarget().getLabel());
//			EdgeElement e = new EdgeElement(src, queryEdge.getLabel(), trg);
//			edges.add(e);
//		}
//		return edges;
//	}

	@Override
	public List<GraphElement> getNeighbors(Map<NodeElement,List<EdgeElement>> graph, Cursor cursor) {
		Set<GraphElement> neighbors = new HashSet<GraphElement>();
		return m_node.getNeighbors(graph, cursor);
//		for (NodeElement node : m_nodes)
//			neighbors.addAll(node.getNeighbors(graph, cursor));
//		neighbors.removeAll(m_nodes);
//		
//		return new ArrayList<GraphElement>(neighbors);
	}

	public String toString() {
		return m_label + "(" + m_node.toString() + ")";
	}
}
