package edu.unika.aifb.graphindex.storage;

public interface TermStorage {
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	public void close() throws StorageException;
	
	public Long getId(String term) throws StorageException;
	public String getTerm(long id) throws StorageException;
	
	public Long add(String term) throws StorageException;
}
