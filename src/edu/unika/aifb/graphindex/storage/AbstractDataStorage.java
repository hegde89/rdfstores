package edu.unika.aifb.graphindex.storage;

public abstract class AbstractDataStorage implements DataStorage {

	protected DataManager m_dataManager;
	protected boolean m_readonly;
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_readonly = readonly;
	}

	public void setDataManager(DataManager dataManager) {
		m_dataManager = dataManager;
	}

}
