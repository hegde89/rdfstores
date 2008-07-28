package edu.unika.aifb.graphindex.graph;

public interface DFSListener {
	public void encounterVertex(int tree, Vertex v, Edge e, int dfsNr);
	public void encounterVertexAgain(int tree, Vertex v, Edge e, int dfsNr);
	public void encounterForwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr);
	public void encounterBackwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr);
	public void treeComplete(int tree);
}
