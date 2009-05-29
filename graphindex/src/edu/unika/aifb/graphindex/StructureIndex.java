package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

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

import edu.unika.aifb.graphindex.storage.ExtensionStorage.DataField;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
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
	private DataManager m_dm;
	private BlockManager m_bm;
	private StatisticsCollector m_collector;
	private int m_configTableCacheSize;
	private int m_configDocCacheSize = 100;
	private Set<String> m_forwardEdges, m_backwardEdges;
	private Map<String,Integer> m_objectCardinalities;
	private List<IndexDescription> m_indexes;

	private boolean m_createIGWithDataNodes = false;
	private boolean m_indexDataNodes = true;
	private int m_pathLength = -1;
	private String m_metaFile;
	
	private static final String META_FILENAME = "index.yml";
	public static final String OPT_IG_WITH_DATA_NODES = "ig_with_data_nodes";
	public static final String OPT_INDEX_DATA_NODES = "index_data_nodes";
	public static final String OPT_PATH_LENGTH = "path_length";
	public static final String OPT_INDEXES = "indexes";
	
	private static final Logger log = Logger.getLogger(StructureIndex.class);

	public StructureIndex(String dir, boolean clean, boolean readonly) throws StorageException {
		m_directory = dir;
		m_metaFile = dir + "/" + META_FILENAME;
		m_collector = new StatisticsCollector();
		m_forwardEdges = new HashSet<String>();
		m_backwardEdges = new HashSet<String>();
		m_objectCardinalities = new HashMap<String,Integer>();
		m_indexes = new ArrayList<IndexDescription>();
		
		initialize(clean, readonly);
	}
	
	private void initialize(boolean clean, boolean readonly) throws StorageException {
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
		
		if (new File(m_metaFile).exists()) {
			try {
				readMetaData();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
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
		try {
			writeMetaData();
		} catch (FileNotFoundException e) {
			throw new StorageException(e);
		}
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

	public boolean indexGraphWithDataNodes() {
		return m_createIGWithDataNodes;
	}
	
	public boolean indexDataNodes() {
		return m_indexDataNodes;
	}
	
	public int getPathLength() {
		return m_pathLength;
	}
	
	public List<IndexDescription> getIndexes() {
		return m_indexes;
	}
	
	public void addIndex(IndexDescription index) {
		m_indexes.add(index);
	}
	
	public void clearIndexes() {
		m_indexes.clear();
	}
	
	public IndexDescription getCompatibleIndex(DataField... fields) {
		for (IndexDescription index : m_indexes)
			if (index.isCompatible(fields))
				return index;
		return null;
	}
	
	public int getDocumentCacheSize() {
		return m_configDocCacheSize ;
	}

	public void setDocumentCacheSize(int docCacheSize) {
		m_configDocCacheSize = docCacheSize;
		m_em.getExtensionStorage().updateCacheSizes();
	}

	private List<IndexDescription> toIndexDescriptions(List<Map<String,String>> indexMaps) {
		List<IndexDescription> indexes = new ArrayList<IndexDescription>();
		for (Map<String,String> map : indexMaps) 
			indexes.add(new IndexDescription(map));
		return indexes;
	}
	

	@SuppressWarnings("unchecked")
	public void setOptions(Map options) throws FileNotFoundException {
		if (options.containsKey(OPT_IG_WITH_DATA_NODES))
			m_createIGWithDataNodes = (Boolean)options.get(OPT_IG_WITH_DATA_NODES);
		if (options.containsKey(OPT_INDEX_DATA_NODES))
			m_indexDataNodes = (Boolean)options.get(OPT_INDEX_DATA_NODES);
		if (options.containsKey(OPT_PATH_LENGTH))
			m_pathLength = (Integer)options.get(OPT_PATH_LENGTH);
		if (options.containsKey(OPT_INDEXES)) {
			m_indexes = toIndexDescriptions((List<Map<String,String>>)options.get(OPT_INDEXES));
		}
		
		writeMetaData();
	}
	
	@SuppressWarnings("unchecked")
	private void readMetaData() throws FileNotFoundException {
		Map meta = (Map)Yaml.load(new File(m_metaFile));
		if (meta.get(OPT_IG_WITH_DATA_NODES) != null)
			m_createIGWithDataNodes = (Boolean)meta.get(OPT_IG_WITH_DATA_NODES);
		if (meta.get(OPT_INDEX_DATA_NODES) != null)
			m_indexDataNodes = (Boolean)meta.get(OPT_INDEX_DATA_NODES);
		if (meta.get(OPT_PATH_LENGTH) != null)
			m_pathLength = (Integer)meta.get(OPT_PATH_LENGTH);
		m_indexes = toIndexDescriptions((List<Map<String,String>>)meta.get(OPT_INDEXES));
	}
	
	@SuppressWarnings("unchecked")
	private void writeMetaData() throws FileNotFoundException {
		Map options = new HashMap();
		options.put(OPT_IG_WITH_DATA_NODES, m_createIGWithDataNodes);
		options.put(OPT_INDEX_DATA_NODES, m_indexDataNodes);
		options.put(OPT_PATH_LENGTH, m_pathLength);
		List<Map<String,String>> indexMaps = new ArrayList<Map<String,String>>();
		for (IndexDescription idx : m_indexes)
			indexMaps.add(idx.asMap());
		options.put(OPT_INDEXES, indexMaps);
		
		Yaml.dump(options, new File(m_metaFile));
	}
}
