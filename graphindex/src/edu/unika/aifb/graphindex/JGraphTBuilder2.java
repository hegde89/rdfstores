package edu.unika.aifb.graphindex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.importer.TripleSink;

public class JGraphTBuilder2 implements TripleSink {
	private DirectedGraph<SVertex,LabeledEdge<SVertex>> m_graph;
	private Map<String,SVertex> vertices = new HashMap<String,SVertex>();
	
	@SuppressWarnings("unchecked")
	public JGraphTBuilder2() {
		m_graph = new DirectedMultigraph<SVertex,LabeledEdge<SVertex>>(new ClassBasedEdgeFactory<SVertex,LabeledEdge<SVertex>>((Class<? extends LabeledEdge<SVertex>>)LabeledEdge.class));
	}
	
	public void triple(String src, String edge, String dst) {
		if (dst.length() > 255)
			dst = dst.substring(0, 254);
		
		SVertex source = vertices.get(src);
		if (source == null) {
			source = new SVertex(src);
			vertices.put(src, source);
			m_graph.addVertex(source);
		}
		
		SVertex dest = vertices.get(dst);
		if (dest == null) {
			dest = new SVertex(dst);
			vertices.put(dst, dest);
			m_graph.addVertex(dest);
		}
		
		m_graph.addEdge(source, dest, new LabeledEdge<SVertex>(source, dest, edge));
	}
	
	public void cleanUp() {
		vertices = null;
	}
	
	public DirectedGraph<SVertex,LabeledEdge<SVertex>> getGraph() {
		return m_graph;
	}
}
