package edu.unika.aifb.graphindex.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractExtensionManager implements ExtensionManager {
	protected Set<String> m_extensionUris;
	protected Map<String,Extension> m_handlers;
	protected ExtensionStorage m_storage;
	protected boolean m_caching;
	protected boolean m_bulk = false;
	
	protected AbstractExtensionManager() {
		m_handlers = new HashMap<String,Extension>();
		m_extensionUris = new HashSet<String>();
	}
	
	public void initialize(boolean clean) throws StorageException {
		m_storage.initialize(clean);
		if (!clean)
			m_extensionUris = m_storage.loadExtensionList();
	}
	
	public void close() throws StorageException {
		m_storage.saveExtensionList(m_extensionUris);
		m_storage.close();
	}

	public void unloadAll() throws StorageException {
		for (Extension e: m_handlers.values())
			e.unload();
		System.gc();
	}
	
	public boolean isCaching() {
		return m_caching;
	}
	
	public void setCaching(boolean caching) {
		m_caching = caching;
	}
	
	public void startBulkUpdate() {
		m_bulk = true;
	}
	
	public void endBulkUpdate() throws StorageException {
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
	
	public boolean registerExtensionHandler(String extUri, Extension extension) {
		if (m_handlers.containsKey(extUri))
			return false;
		m_handlers.put(extUri, extension);
		return true;
	}

	public boolean extensionExists(String extUri) {
		return m_extensionUris.contains(extUri);
	}

	public void join(String leftExt, String leftProperty, String rightExt) {
	}
}
