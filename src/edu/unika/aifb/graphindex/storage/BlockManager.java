package edu.unika.aifb.graphindex.storage;

import edu.unika.aifb.graphindex.StructureIndex;

public interface BlockManager {

	void setBlockStorage(BlockStorage gs);

	void setIndex(StructureIndex index);

	void initialize(boolean clean, boolean readonly) throws StorageException;

	void close() throws StorageException;
	
	public BlockStorage getBlockStorage();

}
