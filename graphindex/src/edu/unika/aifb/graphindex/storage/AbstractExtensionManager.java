package edu.unika.aifb.graphindex.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public abstract class AbstractExtensionManager implements ExtensionManager {
	protected Set<String> m_extensionUris;
	protected Map<String,Extension> m_handlers;
	protected ExtensionStorage m_storage;
	protected boolean m_caching;
	protected boolean m_bulk = false;
	protected int m_mode = ExtensionManager.MODE_NOCACHE;
	
	private static final Logger log = Logger.getLogger(AbstractExtensionManager.class);
	
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
	
	public void removeExtension(String extUri) throws StorageException {
		Extension ext = m_handlers.get(extUri);
		if (ext == null)
			return;
		
		m_handlers.remove(extUri);
		m_extensionUris.remove(extUri);
		ext.remove();
	}
	
	public boolean registerExtensionHandler(String extUri, Extension extension) {
		if (m_handlers.containsKey(extUri))
			return false;
		m_extensionUris.add(extUri);
		m_handlers.put(extUri, extension);
		return true;
	}
	
	public boolean extensionExists(String extUri) {
		return m_extensionUris.contains(extUri);
	}

	public void join(String leftExt, String leftProperty, String rightExt) {
	}
}
