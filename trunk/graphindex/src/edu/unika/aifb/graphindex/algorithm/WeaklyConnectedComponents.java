package edu.unika.aifb.graphindex.algorithm;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

public class WeaklyConnectedComponents<V,E> {
	private Iterator<Set<V>> m_iterator;
	private List<Set<V>> m_components;

	public WeaklyConnectedComponents(DirectedGraph<V,E> graph) {
		ConnectivityInspector<V,E> inspector = new ConnectivityInspector<V,E>(graph);
		m_components = inspector.connectedSets();
		m_iterator = m_components.iterator();
	}
	
	public Set<V> nextComponent() {
		if (!m_iterator.hasNext())
			return null;
		return m_iterator.next();
	}
}
