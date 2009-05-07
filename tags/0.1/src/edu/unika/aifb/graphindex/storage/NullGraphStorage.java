package edu.unika.aifb.graphindex.storage;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

public class NullGraphStorage extends AbstractGraphStorage {

	public void initialize(boolean clean) throws StorageException {
	}

	public void close() throws StorageException {
	}

	public Set<String> loadGraphList() throws StorageException {
		return new HashSet<String>();
	}

	public void saveGraphList(Set<String> graphs) throws StorageException {
	}

	public Set<LabeledEdge<String>> loadEdges(String graphName) throws StorageException {
		return new HashSet<LabeledEdge<String>>();
	}

	public void saveEdges(String graphName, Set<LabeledEdge<String>> edges) throws StorageException {
	}

	public void optimize() throws StorageException {
		
	}

	public void addEdge(String graphName, String source, String edge, String target)
			throws StorageException {
	}

	public IndexReader getIndexReader() {
		return null;
	}

	public IndexSearcher getIndexSearcher() {
		return null;
	}
}
