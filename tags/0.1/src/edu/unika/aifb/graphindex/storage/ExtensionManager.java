package edu.unika.aifb.graphindex.storage;

import edu.unika.aifb.graphindex.StructureIndex;

public interface ExtensionManager {
	public static final int MODE_NOCACHE = 0;
	public static final int MODE_READONLY = 2;
	
	public void initialize(boolean clean, boolean readonly) throws StorageException;	
	public void close() throws StorageException;
	public void setIndex(StructureIndex structureIndex);
	public StructureIndex getIndex();
	
	public ExtensionStorage getExtensionStorage();
	public void setExtensionStorage(ExtensionStorage es);
	
	public int getMode();
	public void setMode(int mode);
	public void flushAllCaches() throws StorageException;
	
	public void startBulkUpdate() throws StorageException;
	public void finishBulkUpdate() throws StorageException;
	public boolean bulkUpdating();
}
