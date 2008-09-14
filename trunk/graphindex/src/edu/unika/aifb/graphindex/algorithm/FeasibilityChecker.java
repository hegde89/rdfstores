package edu.unika.aifb.graphindex.algorithm;

import java.util.Map;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;

public interface FeasibilityChecker<V,E,G extends DirectedGraph<V,E>> {
	
	public boolean isEdgeCompatible(E e1, E e2);
	
	public boolean isVertexCompatible(V n1, V n2);
}
