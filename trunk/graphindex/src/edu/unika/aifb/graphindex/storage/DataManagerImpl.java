package edu.unika.aifb.graphindex.storage;

import java.util.Set;

import edu.unika.aifb.graphindex.StructureIndex;



public class DataManagerImpl implements DataManager {
	
	protected DataStorage m_ds;
	protected StructureIndex m_index;
	protected int m_id = 0;
	
	public DataManagerImpl() {
		
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_ds.initialize(clean, readonly);
	}

	public void close() throws StorageException {
		m_ds.close();
	}

	public void setIndex(StructureIndex index) {
		m_index = index;
	}
	
	public StructureIndex getIndex() {
		return m_index;
	}

	public void setDataStorage(DataStorage bs) {
		m_ds = bs;
	}

	public DataStorage getDataStorage() {
		return m_ds;
	}

}
