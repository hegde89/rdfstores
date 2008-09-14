package edu.unika.aifb.graphindex.storage;

public abstract class AbstractGraphStorage implements GraphStorage {

	protected GraphManager m_graphManager;
	protected boolean m_readonly;
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_readonly = readonly;
	}

	public void setGraphManager(GraphManager graphManager) {
		m_graphManager = graphManager;
	}

}
