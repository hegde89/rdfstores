package edu.unika.aifb.graphindex.algorithm;

import java.util.HashSet;
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
	
	public int size() {
		return m_components.size();
	}
	
	public Set<V> nextComponent() {
		if (!m_iterator.hasNext())
			return null;
		Set<V> c = m_iterator.next();
		m_iterator.remove();
		return new HashSet<V>(c);
	}
}
