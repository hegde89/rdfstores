package edu.unika.aifb.graphindex;

import java.io.File;

import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;

public class StructureIndex {
	private String m_directory;
	private ExtensionManager m_em;
	private GraphManager m_gm;
	private StatisticsCollector m_collector;
	private int m_configTableCacheSize;
	private boolean m_gzip = false;
	private int m_configDocCacheSize = 100;
	
	public StructureIndex(String dir, boolean clean, boolean readonly) throws StorageException {
		m_directory = dir;
		m_collector = new StatisticsCollector();
		
		initialize(clean, readonly);
	}
	
	private void initialize(boolean clean, boolean readonly) throws StorageException {
		if (new File(m_directory + "/gzip").exists())
			m_gzip = true;

		ExtensionStorage es = new LuceneExtensionStorage(m_directory + "/index");
		m_em = new LuceneExtensionManager();
		m_em.setExtensionStorage(es);
		m_em.setIndex(this);
		m_em.initialize(clean, readonly);
		
		GraphStorage gs = new LuceneGraphStorage(m_directory + "/graph");
		m_gm = new GraphManagerImpl();
		m_gm.setGraphStorage(gs);
		m_gm.setIndex(this);
		m_gm.initialize(clean, readonly);
		
	}
	
	public StatisticsCollector getCollector() {
		return m_collector;
	}
	
	public ExtensionManager getExtensionManager() {
		return m_em;
	}
	
	public GraphManager getGraphManager() {
		return m_gm;
	}
	
	public void optimize() {
	}

	public void close() throws StorageException {
		m_em.close();
		m_gm.close();
	}
	
	public int getTableCacheSize() {
		return m_configTableCacheSize;
	}
	
	public void setTableCacheSize(int n) {
		m_configTableCacheSize = n;
		m_em.getExtensionStorage().updateCacheSizes();
	}
	
	public void clearCaches() throws StorageException {
		m_em.getExtensionStorage().clearCaches();
	}

	public boolean isGZip() {
		return m_gzip;
	}

	public int getDocumentCacheSize() {
		return m_configDocCacheSize ;
	}

	public void setDocumentCacheSize(int docCacheSize) {
		m_configDocCacheSize = docCacheSize;
		m_em.getExtensionStorage().updateCacheSizes();
	}
}
