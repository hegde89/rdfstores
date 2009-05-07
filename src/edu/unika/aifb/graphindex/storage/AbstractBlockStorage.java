package edu.unika.aifb.graphindex.storage;

public abstract class AbstractBlockStorage implements BlockStorage {

	protected BlockManager m_blockManager;
	protected boolean m_readonly;
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_readonly = readonly;
	}

	public void setBlockManager(BlockManager dataManager) {
		m_blockManager = dataManager;
	}

}
