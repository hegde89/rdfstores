package edu.unika.aifb.graphindex.storage;

import java.util.Set;

public interface ExtensionStorage {
	public void initialize(boolean clean);
	public void close();
	
	public void finishBulkUpdate() throws StorageException;
	
	public Set<String> loadExtensionList() throws StorageException;
	public void saveExtensionList(Set<String> uris) throws StorageException;
	public void setExtensionManager(ExtensionManager extensionManager);
}
