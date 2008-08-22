package edu.unika.aifb.graphindex.storage;

public abstract class AbstractGraphStorage implements GraphStorage {

	protected GraphManager m_graphManager;
	
	public void initialize(boolean clean) throws StorageException {

	}

	public void setGraphManager(GraphManager graphManager) {
		m_graphManager = graphManager;
	}

}
