package edu.unika.aifb.graphindex.storage;

public class StorageManager {
	private ExtensionManager m_extManager;
	
	private static StorageManager m_instance = null;
	
	private StorageManager() {
		
	}
	
	public static StorageManager getInstance() {
		if (m_instance == null)
			m_instance = new StorageManager();
		return m_instance;
	}
	
	public ExtensionManager getExtensionManager() {
		return m_extManager;
	}

	public void setExtensionManager(ExtensionManager manager) {
		m_extManager = manager;
	}

}
