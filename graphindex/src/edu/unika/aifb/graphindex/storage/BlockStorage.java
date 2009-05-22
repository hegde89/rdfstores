package edu.unika.aifb.graphindex.storage;

import org.apache.lucene.search.IndexSearcher;

public interface BlockStorage {
	
	public final String BLOCK_FIELD = "block";
	public final String ELE_FIELD = "ele";
	
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	
	public void close() throws StorageException;

	public void optimize() throws StorageException;

	public void addBlock(String block, String element)throws StorageException;
	
	public IndexSearcher getIndexSearcher();
}
