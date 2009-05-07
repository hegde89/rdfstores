package edu.unika.aifb.graphindex.storage;

import java.util.Set;

import edu.unika.aifb.graphindex.StructureIndex;



public class BlockManagerImpl implements BlockManager {
	
	protected BlockStorage m_bs;
	protected StructureIndex m_index;
	protected int m_id = 0;
	
	public BlockManagerImpl() {
		
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_bs.initialize(clean, readonly);
	}

	public void close() throws StorageException {
		m_bs.close();
	}

	public void setIndex(StructureIndex index) {
		m_index = index;
	}
	
	public StructureIndex getIndex() {
		return m_index;
	}

	public void setBlockStorage(BlockStorage bs) {
		m_bs = bs;
	}

	public BlockStorage getBlockStorage() {
		return m_bs;
	}

}
