package edu.unika.aifb.graphindex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphElement;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.Path;
import edu.unika.aifb.graphindex.graph.Vertex;

public class Index implements Serializable {
	private GraphManager m_gm = GraphManager.getInstance(); 
	private List<Graph> m_indexGraphs;
	
	private static final long serialVersionUID = -2170078304208007162L;
	private static final Logger log = Logger.getLogger(Index.class);
	
	public Index() {
		m_indexGraphs = new ArrayList<Graph>();
	}
	
	public Index(List<Graph> graphs) {
		m_indexGraphs = graphs;
	}
	
	public void load() {
		for (String name : m_gm.getStoredGraphs()) {
			Graph g = GraphFactory.graph(name);
			g.load();
			m_indexGraphs.add(g);
		}
	}
	
	public List<Graph> getIndexGraphs() {
		return m_indexGraphs;
	}
}
