package edu.unika.aifb.graphindex.algorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.extensions.ExtEntry;
import edu.unika.aifb.graphindex.extensions.Extension;
import edu.unika.aifb.graphindex.extensions.ExtensionManager;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphElement;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.Vertex;

public class DGBuilder {

	private ExtensionManager m_em = ExtensionManager.getInstance();
	private Map<Set<Vertex>,Vertex> m_targetHash;
	private int m_nidx = 0;

	private static final Logger log = Logger.getLogger(DGBuilder.class);

	public DGBuilder() {
		m_targetHash = new HashMap<Set<Vertex>,Vertex>();
	}
	
	private void makeDataGuide(Set<Vertex> t1, Vertex d1) {
		Map<String,Set<Vertex>> p = new HashMap<String,Set<Vertex>>();
//		Map<String,Set<ExtEntry>> exts = new HashMap<String,Set<ExtEntry>>();
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
				
//				Set<ExtEntry> ext = exts.get(e.getLabel());
//				if (ext == null) {
//					ext = new HashSet<ExtEntry>();
//					exts.put(e.getLabel(), ext);
//				}
				Set<String[]> entries = exts.get(e.getLabel());
				if (entries == null) {
					entries = new HashSet<String[]>();
					exts.put(e.getLabel(), entries);
				}
//				ExtEntry ee = new ExtEntry(e.getTarget().getLabel());
//				ee.addParent(v.getLabel());
//				ext.add(ee);
				entries.add(new String [] { e.getTarget().getLabel(), e.getLabel(), v.getLabel() });
			}
		}
		
		for (String l : p.keySet()) {
			Set<Vertex> t2 = p.get(l);
			Vertex d2 = m_targetHash.get(t2);
			if (d2 != null) {
				d1.getGraph().addEdge(new Edge(d1, d2, l));
//				m_em.getExtension(d2.getLabel()).addAll(l, exts.get(l));
				Extension ext = m_em.getExtension(d2.getLabel());
				for (String[] t : exts.get(l))
					ext.add(t[0], t[1], t[2]);
			}
			else {
				d2 = new Vertex("dgO" + ++m_nidx);
				
				m_targetHash.put(t2, d2);
//				m_em.getExtension(d2.getLabel()).addAll(l, exts.get(l));
				Extension ext = m_em.getExtension(d2.getLabel());
				for (String[] t : exts.get(l))
					ext.add(t[0], t[1], t[2]);

				d1.getGraph().addVertex(d2);
				d1.getGraph().addEdge(new Edge(d1, d2, l));
				
				makeDataGuide(t2, d2);
			}
		}
	}
	
	public Graph buildDataGuide(Graph sourceGraph) {
		Graph dataGuide = GraphFactory.graph();
		
		Vertex dgRoot = new Vertex("dgR" + ++m_nidx);
//		m_em.getExtension(dgRoot.getLabel()).add("", new ExtEntry(sourceGraph.getRoot().getLabel()));
		m_em.getExtension(dgRoot.getLabel()).add(sourceGraph.getRoot().getLabel(), "", null);
		
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
