package edu.unika.aifb.graphindex.storage;

import java.util.Set;

public abstract class AbstractExtension implements Extension {

	protected String m_uri;
	protected ExtensionManager m_manager;
	
	public AbstractExtension(String uri, ExtensionManager manager) {
		m_uri = uri;
		m_manager = manager;
	}
	
	public String getUri() {
		return m_uri;
	}
	
	protected int getMode() {
		return m_manager.getMode();
	}
}
