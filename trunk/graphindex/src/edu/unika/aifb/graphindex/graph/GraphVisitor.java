package edu.unika.aifb.graphindex.graph;

public interface GraphVisitor {
	public void visit(Vertex v);
	public void visit(Edge e);
}
