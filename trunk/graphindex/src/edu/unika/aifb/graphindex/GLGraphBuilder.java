package edu.unika.aifb.graphindex;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.Graph;

public class GLGraphBuilder implements GraphBuilder {
	
	private Graph m_graph;
	
	private static final Logger log = Logger.getLogger(GLGraphBuilder.class);
	
	public GLGraphBuilder() {
		m_graph = new Graph("graph");
	}

	public void addTriple(String source, String label, String target) {
		if (target.length() > 255)
			target = target.substring(0, 254);
		m_graph.addEdge(source, label, target);
	}
	
	public Graph getGraph() {
		return m_graph;
	}
}
