package edu.unika.aifb.graphindex.storage;

public interface ExtensionManager {
	public void initialize(boolean clean) throws StorageException;
	public void unloadAll() throws StorageException;
	public void close() throws StorageException;
	
	public ExtensionStorage getExtensionStorage();
	public void setExtensionStorage(ExtensionStorage es);
	
	public Extension extension(String extUri) throws StorageException;
	public boolean extensionExists(String extUri);
	
	public boolean registerExtensionHandler(String extUri, Extension handler);
	
	public boolean isCaching();
	public void setCaching(boolean caching);
	public void join(String leftExt, String leftProperty, String rightExt);
	
	public void startBulkUpdate();
	public void endBulkUpdate() throws StorageException;
	public boolean bulkUpdating();
}
