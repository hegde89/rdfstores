package edu.unika.aifb.graphindex.algorithm.rcp.generic;

public interface LabelProvider<V,E> {
	public String getVertexLabel(V vertex);
	public String getEdgeLabel(E edge);
}
