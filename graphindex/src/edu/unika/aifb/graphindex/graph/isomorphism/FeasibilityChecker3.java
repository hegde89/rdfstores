package edu.unika.aifb.graphindex.graph.isomorphism;

import edu.unika.aifb.graphindex.graph.IndexEdge;

public interface FeasibilityChecker3 {
	public boolean isEdgeCompatible(IndexEdge e1, IndexEdge e2);
	
	public boolean isVertexCompatible(int n1, int n2);
	
	public boolean checkVertexCompatible(int n1, int n2);
}
