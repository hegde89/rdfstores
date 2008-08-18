package edu.unika.aifb.graphindex.storage;

public interface ExtensionManager {
	public static final int MODE_NOCACHE = 0;
	public static final int MODE_WRITECACHE = 1;
	public static final int MODE_READONLY = 2;
	
	public void initialize(boolean clean, boolean readonly) throws StorageException;	
	public void close() throws StorageException;
	
	public ExtensionStorage getExtensionStorage();
	public void setExtensionStorage(ExtensionStorage es);
	
	public Extension extension(String extUri) throws StorageException;
	public boolean extensionExists(String extUri);
	public void removeExtension(String extUri) throws StorageException;
	
	public boolean registerExtensionHandler(String extUri, Extension handler);
	
	public int getMode();
	public void setMode(int mode);
	public void flushAllCaches() throws StorageException;
	
	public void join(String leftExt, String leftProperty, String rightExt);
	
	public void startBulkUpdate() throws StorageException;
	public void finishBulkUpdate() throws StorageException;
	public boolean bulkUpdating();
}
