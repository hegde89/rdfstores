package edu.unika.aifb.graphindex;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.BlockManager;
import edu.unika.aifb.graphindex.storage.BlockManagerImpl;
import edu.unika.aifb.graphindex.storage.BlockStorage;
import edu.unika.aifb.graphindex.storage.DataManager;
import edu.unika.aifb.graphindex.storage.DataManagerImpl;
import edu.unika.aifb.graphindex.storage.DataStorage;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

import edu.unika.aifb.graphindex.storage.lucene.LuceneBlockStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneDataStorage;
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
	private Set<String> m_forwardEdges, m_backwardEdges;
	private Map<String,Integer> m_objectCardinalities;
	
	private DataManager m_dm;
	private BlockManager m_bm;
	
	public StructureIndex(String dir, boolean clean, boolean readonly) throws StorageException {
		m_directory = dir;
		m_collector = new StatisticsCollector();
		m_forwardEdges = new HashSet<String>();
		m_backwardEdges = new HashSet<String>();
		m_objectCardinalities = new HashMap<String,Integer>();
		
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
		
		DataStorage ds = new LuceneDataStorage(m_directory + "/data");
		m_dm = new DataManagerImpl();
		m_dm.setDataStorage(ds);
		m_dm.setIndex(this);
		m_dm.initialize(clean, readonly);
		
		BlockStorage bs = new LuceneBlockStorage(m_directory + "/block");
		m_bm = new BlockManagerImpl();
		m_bm.setBlockStorage(bs);
		m_bm.setIndex(this);
		m_bm.initialize(clean, readonly);
	}
	
	public void setObjectCardinalities(Map<String,Integer> cards) {
		m_objectCardinalities = cards;
	}
	
	public Integer getObjectCardinality(String property) {
		return m_objectCardinalities.get(property);
	}
	
	public Set<String> getForwardEdges() {
		return m_forwardEdges;
	}

	public void setForwardEdges(Set<String> edges) {
		m_forwardEdges = edges;
	}

	public Set<String> getBackwardEdges() {
		return m_backwardEdges;
	}

	public void setBackwardEdges(Set<String> edges) {
		m_backwardEdges = edges;
	}
	
	public String getDirectory() {
		return m_directory;
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
	
	public DataManager getDataManager() {
		return m_dm;
	}
	
	public BlockManager getBlockManager() {
		return m_bm;
	}
	
	public void optimize() {
	}

	public void close() throws StorageException {
		m_em.close();
		m_gm.close();
		m_dm.close();
		m_bm.close();
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
