package edu.unika.aifb.graphindex.storage;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Triple;


public interface ExtensionStorage {
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	public void close() throws StorageException;
	
	public void startBulkUpdate() throws StorageException;
	public void finishBulkUpdate() throws StorageException;
	
	public Set<String> loadExtensionList() throws StorageException;
	public void saveExtensionList(Set<String> uris) throws StorageException;
	public void setExtensionManager(ExtensionManager extensionManager);
	public void optimize() throws StorageException;
	
	public boolean hasTriples(String ext, String propertyUri, String object) throws StorageException;
//	public List<Triple> getTriples(String extUri, String property, String object) throws StorageException;
//	public List<Triple> getTriples(String extUri, String property) throws StorageException;
//	public GTable<String> getTable(String extUri, String property, String object, String allowedSubject) throws StorageException;
	
	public void mergeExtensions() throws IOException, StorageException;
	
	public void clearCaches() throws StorageException;
}
