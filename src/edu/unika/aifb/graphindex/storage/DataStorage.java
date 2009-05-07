package edu.unika.aifb.graphindex.storage;

import org.apache.lucene.search.IndexSearcher;

public interface DataStorage {
	
	public static final String SRC_FIELD = "src";
	public static final String EDGE_FIELD = "edge";
	public static final String DST_FIELD = "dst";
	public static final String TYPE_FIELD = "type";  
	
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	
	public void close() throws StorageException;

	public void optimize() throws StorageException;

	public void  addTriple(String src, String edge, String dst, String type) throws StorageException;
	
	public IndexSearcher getIndexSearcher();
}
