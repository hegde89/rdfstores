package edu.unika.aifb.graphindex.test;

import java.util.Map;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;

public interface FeasibilityChecker<V,E,G extends DirectedGraph<V,E>> {
	public boolean isSemanticallyFeasible(G g1, G g2, V n1, V n2, Map<V,V> core1, Map<V,V> core2);
}
