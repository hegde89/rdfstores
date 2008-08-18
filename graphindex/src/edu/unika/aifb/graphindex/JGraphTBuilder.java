package edu.unika.aifb.graphindex;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

public class JGraphTBuilder {
	private DirectedGraph<String,LabeledEdge> m_graph;
	
	public JGraphTBuilder() {
		m_graph = new DefaultDirectedGraph<String,LabeledEdge>(new ClassBasedEdgeFactory<String,LabeledEdge>(LabeledEdge.class));
	}
	
	public void addTriple(String src, String edge, String dst) {
		if (dst.length() > 255)
			dst = dst.substring(0, 254);
		
		m_graph.addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	public DirectedGraph<String,LabeledEdge> getGraph() {
		return m_graph;
	}
}
