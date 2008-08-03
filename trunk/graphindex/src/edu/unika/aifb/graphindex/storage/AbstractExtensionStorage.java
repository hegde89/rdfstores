package edu.unika.aifb.graphindex.storage;

import java.util.Set;

public abstract class AbstractExtensionStorage implements ExtensionStorage {

	protected ExtensionManager m_manager;
	
	public void setExtensionManager(ExtensionManager extensionManager) {
		m_manager = extensionManager;
	}

	public boolean bulkUpdating() {
		return m_manager.bulkUpdating();
	}
}
