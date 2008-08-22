package edu.unika.aifb.graphindex.storage;

import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;

public interface GraphManager {
	public void initialize(boolean clean) throws StorageException;	
	public void close() throws StorageException;
	
	public GraphStorage getGraphStorage();
	public void setGraphStorage(GraphStorage gs);
	
	public NamedGraph<String,LabeledEdge<String>> graph(String graphName) throws StorageException;
	public NamedGraph<String,LabeledEdge<String>> graph() throws StorageException;
}
