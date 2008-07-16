package edu.unika.aifb.graphindex.algorithm;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphElement;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.Vertex;
import edu.unika.aifb.graphindex.query.QueryVertex;

public class Partitioner {

	private Graph m_graph;
//	private Set<Graph> m_partitions;
	private Graph m_cur;
	private int m_partCount;
	private Set<Edge> m_dfsMarks;
	private Set<Vertex> m_unmarkedVertices;
	private String m_graphClass;
	private final String DEFAULT_GRAPH_CLASS = "edu.unika.aifb.graphindex.graph.Graph";
	
	private static final Logger log = Logger.getLogger(Partitioner.class);
	
	public Partitioner(Graph g) {
		m_graph = g;
		m_graphClass = DEFAULT_GRAPH_CLASS;

		m_unmarkedVertices = new HashSet<Vertex>(g.vertices());
	}
	
	public Partitioner(Graph g, String graphClass) {
		this(g);
		m_graphClass = graphClass;
//		m_vertexClass = vertexClass;
	}
	
	private void mark(Vertex v) {
		m_unmarkedVertices.remove(v);
		for (Edge e : v.outgoingEdges()) {
			if (m_dfsMarks.contains(e))
				continue;
			m_dfsMarks.add(e);
			
			if (e.getSource() instanceof QueryVertex) {
				m_cur.addVertex(new QueryVertex(e.getSource()));
			}
			if (e.getTarget() instanceof QueryVertex) {
				m_cur.addVertex(new QueryVertex(e.getTarget()));
			}
			
			m_cur.addEdge(e.getSource().getLabel(), e.getLabel(), e.getTarget().getLabel());
			
			mark(e.getTarget());
		}
	}
	
	private Vertex selectStartNode(Graph g) {
		Vertex start = null;
		if (m_unmarkedVertices.size() > 0) {
			for (Vertex v : m_unmarkedVertices) {
				if (g.outDegreeOf(v) > 0 && g.inDegreeOf(v) == 0) {
					start = v;
					break;
				}
			}
			
			if (start == null) {
				for (Vertex v : m_unmarkedVertices) {
					if (g.outDegreeOf(v) > 0) {
						start = v;
						break;
					}
				}
			}
			
			if (start == null)
				log.error("wtf");
		}
		else
			start = null;
		
		return start;
	}
	
	private Graph getNewGraph() {
		return GraphFactory.graphByClass(m_graphClass, false);
	}
	
	public Graph nextPartition() {
		Vertex start = selectStartNode(m_graph);
		
		if (start == null)
			return null;

		m_cur = getNewGraph();
		m_cur.addVertex((Vertex)start.clone());
		m_cur.setRoot(start.getLabel());
		m_dfsMarks = new HashSet<Edge>();
		
		mark(start);
		
		m_partCount++;
		
//		if (m_cur.numberOfVertices() < min)
//			min = m_cur.numberOfVertices();
//		if (m_cur.numberOfVertices() > max)
//			max = m_cur.numberOfVertices();
//		avg += m_cur.numberOfVertices();
		
		if (m_partCount % 5000 == 0) {
			log.debug("partitions created: " + m_partCount + ", unmarked left: " + m_unmarkedVertices.size());
		}
		
		return m_cur;
	}
	
//	public Set<Graph> partition(Graph g) {
//		m_partitions = new HashSet<Graph>();
//		m_unmarkedVertices = new HashSet<Vertex>(g.vertices());
//
//		log.debug("total vertices: " + g.numberOfVertices());
//		Vertex start;
//		int partCount = 0;
//		
//		int min = Integer.MAX_VALUE, max = 0;
//		double avg = 0;
//		
//		while ((start = selectStartNode(g)) != null) {
////			log.debug(start + " " + m_unmarkedVertices.size());
//			m_cur = getNewGraph();
//			m_cur.setName("partition_" + partCount);
//			m_cur.addVertex((Vertex)start.clone());
//			m_cur.setRoot(start.getLabel());
//			m_dfsMarks = new HashSet<Edge>();
//			
//			mark(start);
//			
//			m_partitions.add(m_cur);
//			m_cur.setId(partCount);
//			partCount++;
//			
//			if (m_cur.numberOfVertices() < min)
//				min = m_cur.numberOfVertices();
//			if (m_cur.numberOfVertices() > max)
//				max = m_cur.numberOfVertices();
//			avg += m_cur.numberOfVertices();
//			
//			if (partCount % 5000 == 0) {
//				log.debug("partitions created: " + partCount + ", unmarked left: " + m_unmarkedVertices.size());
//				log.debug(Runtime.getRuntime().maxMemory() / 1000 + " " + Runtime.getRuntime().totalMemory() / 1000 + " " + Runtime.getRuntime().freeMemory() / 1000);
//			}
////			Util.printDOT("part" + partCount + ".dot", m_cur);
//		}
//		avg /= m_partitions.size();
//		log.info("partition size: min: " + min + ", max: " + max + ", avg: " + avg);
//		
//		return m_partitions;
//	}
}
