package edu.unika.aifb.graphindex.storage;

import edu.unika.aifb.graphindex.StructureIndex;

public interface DataManager {

	void setDataStorage(DataStorage gs);

	void setIndex(StructureIndex index);

	void initialize(boolean clean, boolean readonly) throws StorageException;

	void close() throws StorageException;
	
	public DataStorage getDataStorage();

}
