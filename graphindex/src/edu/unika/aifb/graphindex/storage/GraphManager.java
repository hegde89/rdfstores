package edu.unika.aifb.graphindex.storage;

import java.util.Set;

import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;

public interface GraphManager {
	public void initialize(boolean clean, boolean readonly) throws StorageException;	
	public void close() throws StorageException;
	
	public GraphStorage getGraphStorage();
	public void setGraphStorage(GraphStorage gs);
	
	public Set<String> getStoredGraphs() throws StorageException;
	
	public NamedGraph<String,LabeledEdge<String>> graph(String graphName) throws StorageException;
	public NamedGraph<String,LabeledEdge<String>> graph() throws StorageException;
}
