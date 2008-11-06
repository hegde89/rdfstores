package edu.unika.aifb.graphindex.storage;

import java.util.Set;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

public interface GraphStorage {
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	public void close() throws StorageException;

	public void setGraphManager(GraphManager graphManager);
	
//	public void startBulkUpdate() throws StorageException;
//	public void finishBulkUpdate() throws StorageException;
	
	public Set<LabeledEdge<String>> loadEdges(String graphName) throws StorageException;
	public void saveEdges(String graphName, Set<LabeledEdge<String>> edges) throws StorageException;
	public void addEdge(String graphName, String source, String edge, String target) throws StorageException;
	
	public Set<String> loadGraphList() throws StorageException;
	public void saveGraphList(Set<String> graphs) throws StorageException;
	public void optimize() throws StorageException;
}
