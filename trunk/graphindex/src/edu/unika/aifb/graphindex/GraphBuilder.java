package edu.unika.aifb.graphindex;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.Graph;

public class GraphBuilder {
	
	private Graph m_graph;
	private boolean m_transpose;
	
	private static final Logger log = Logger.getLogger(GraphBuilder.class);
	
	public GraphBuilder() {
		this(false);
	}
	
	public GraphBuilder(boolean transpose) {
		m_graph = new Graph("graph");
		m_transpose = transpose;
	}
	
	public void addTriple(String source, String label, String target) {
//		log.debug(source + " " + label + " " + target);
		if (target.length() > 255) {
//			log.debug(target);
			target = target.substring(0, 254);
		}
		if (m_transpose)
			m_graph.addEdge(target, label, source);
		else
			m_graph.addEdge(source, label, target);
		
	}
	
	public Graph getGraph() {
		return m_graph;
	}
}
