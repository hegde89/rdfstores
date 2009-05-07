package edu.unika.aifb.graphindex.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;

public abstract class AbstractExtensionManager implements ExtensionManager {
	protected Set<String> m_extensionUris;
	protected ExtensionStorage m_storage;
	protected StructureIndex m_index;
	protected boolean m_readonly;
	protected boolean m_bulk = false;
	protected int m_mode = ExtensionManager.MODE_NOCACHE;
	
	private static final Logger log = Logger.getLogger(AbstractExtensionManager.class);
	
	protected AbstractExtensionManager() {
		m_extensionUris = new HashSet<String>();
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_readonly = readonly;
		m_storage.initialize(clean, readonly);
		if (!clean)
			m_extensionUris = m_storage.loadExtensionList();
	}
	
	public void close() throws StorageException {
		if (getMode() != ExtensionManager.MODE_READONLY)
			m_storage.saveExtensionList(m_extensionUris);
		m_storage.close();
	}
	
	public void setIndex(StructureIndex index) {
		m_index = index;
	}
	
	public StructureIndex getIndex() {
		return m_index;
	}

	public void setMode(int mode) {
		m_mode = mode;
	}
	
	public int getMode() {
		return m_mode;
	}
	
	public void startBulkUpdate() throws StorageException {
		m_bulk = true;
		m_storage.startBulkUpdate();
	}
	
	public void finishBulkUpdate() throws StorageException {
		m_bulk = false;
		m_storage.finishBulkUpdate();
	}
	
	public boolean bulkUpdating() {
		return m_bulk;
	}

	public ExtensionStorage getExtensionStorage() {
		return m_storage;
	}
	
	public void setExtensionStorage(ExtensionStorage storage) {
		m_storage = storage;
		m_storage.setExtensionManager(this);
	}
}
