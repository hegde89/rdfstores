package edu.unika.aifb.graphindex.algorithm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.Vertex;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class DGBuilder {

	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	private Map<Set<Vertex>,Vertex> m_targetHash;
	private int m_nidx = 0;

	private static final Logger log = Logger.getLogger(DGBuilder.class);

	public DGBuilder() {
		m_targetHash = new HashMap<Set<Vertex>,Vertex>();
	}
	
	private void makeDataGuide(Set<Vertex> t1, Vertex d1) throws StorageException {
//		log.debug(t1.size() + " " + d1);
		Map<String,Set<Vertex>> p = new HashMap<String,Set<Vertex>>();
		Map<String,Set<String[]>> exts = new HashMap<String,Set<String[]>>();
		for (Vertex v : t1) {
			Set<Edge> out = v.outgoingEdges();
			for (Edge e : out) {
				Set<Vertex> targets = p.get(e.getLabel());
				if (targets == null) {
					targets = new HashSet<Vertex>();
					p.put(e.getLabel(), targets);
				}
				targets.add(e.getTarget());
				
				Set<String[]> entries = exts.get(e.getLabel());
				if (entries == null) {
					entries = new HashSet<String[]>();
					exts.put(e.getLabel(), entries);
				}
				entries.add(new String [] { e.getTarget().getLabel(), e.getLabel(), v.getLabel() });
			}
		}
		
		for (String l : p.keySet()) {
			Set<Vertex> t2 = p.get(l);
			Vertex d2 = m_targetHash.get(t2);
			if (d2 != null) {
				d1.getGraph().addEdge(new Edge(d1, d2, l));
				Extension ext = m_em.extension(d2.getLabel());
				for (String[] t : exts.get(l))
					ext.addTriple(t[2], t[1], t[0]);
			}
			else {
				d2 = new Vertex("dgo" + ++m_nidx);
				
				m_targetHash.put(t2, d2);
				Extension ext = m_em.extension(d2.getLabel());
				for (String[] t : exts.get(l))
					ext.addTriple(t[2], t[1], t[0]);

				d1.getGraph().addVertex(d2);
				d1.getGraph().addEdge(new Edge(d1, d2, l));
				
				makeDataGuide(t2, d2);
			}
		}
	}
	
	public Graph buildDataGuide(Graph sourceGraph) throws StorageException {
		Graph dataGuide = GraphFactory.graph();
		
		Vertex dgRoot = new Vertex("dgr" + ++m_nidx);
		m_em.extension(dgRoot.getLabel()).addTriple("", "", sourceGraph.getRoot().getLabel());
		
		dataGuide.addVertex(dgRoot);
		dataGuide.setRoot(dgRoot);
		
		Set<Vertex> startVertices = new HashSet<Vertex>();
		startVertices.add(sourceGraph.getRoot());
		
		m_targetHash.clear();
		m_targetHash.put(startVertices, dgRoot);
		
		makeDataGuide(startVertices, dgRoot);
		
		return dataGuide;
	}
}
