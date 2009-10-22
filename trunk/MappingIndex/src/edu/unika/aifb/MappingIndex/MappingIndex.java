package edu.unika.aifb.MappingIndex;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.index.*;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;

public class MappingIndex extends Index {
	
	private Map<IndexDescription,IndexStorage> m_indexes;
	
	// Source data source
	//private String m_ds_source;
	// Target data source
	//private String m_ds_destination;
	// Mapping Directory
	//private String m_mapping_dir;
	// Output Directory
	private String m_root_dir;

	public MappingIndex(String idxDirectory, IndexConfiguration idxConfig) throws IOException, StorageException {
		super(new IndexDirectory(idxDirectory), idxConfig);
		m_indexes = new HashMap<IndexDescription, IndexStorage>();
		//m_ds_source = source;
		//m_ds_destination = target;
		m_root_dir = idxDirectory;
		openAllIndexes();
	}
	
	public void close() throws StorageException {
		for (IndexStorage is : m_indexes.values())
			is.close();
		m_indexes.clear();
	}
	
	private void openAllIndexes() throws StorageException {
		for (IndexDescription index : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES))
			getIndexStorage(index);
	}
	
	public IndexStorage getIndexStorage(IndexDescription index) throws StorageException {
		IndexStorage is = m_indexes.get(index);
		if (is == null) {
			//try {
				// Get directory name for this mapping out of the name of both data sources
				//m_mapping_dir = m_ds_source.replaceAll("[_[^\\w\\d]]", "") + "_" + m_ds_destination.replaceAll("[_[^\\w\\d]]", "");
				
				is = new LuceneIndexStorage(new File(new File(m_root_dir + "/mp").getAbsolutePath() + "/" + index.getIndexFieldName()), m_idxReader != null ? m_idxReader.getCollector() : new StatisticsCollector());
				is.initialize(false, true);				
				m_indexes.put(index, is);
			//} catch (IOException e) {
			//	throw new StorageException(e);
			//}
		}
		return is;
	}
	
	public Set<String> getStoTMapping(String ds_source, String ds_target, String e_source) throws StorageException {
		IndexDescription index = IndexDescription.DSDTESET;
		return  getIndexStorage(index).getDataSet(index, DataField.E_TARGET, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_SOURCE, e_source));
	}
	
	public Set<String> getTtoSMapping(String ds_source, String ds_target, String e_target) throws StorageException {
		IndexDescription index = IndexDescription.DSDTETES;
		return  getIndexStorage(index).getDataSet(index, DataField.E_SOURCE, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_TARGET, e_target));
	}
	
	public Set<String> getStoTExtMapping(String ds_source, String ds_target, String ext_source) throws StorageException {
		IndexDescription index = IndexDescription.DSDTESXETX;
		return  getIndexStorage(index).getDataSet(index, DataField.E_TARGET_EXT, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_SOURCE_EXT, ext_source));
	}
	
	public Set<String> getTtoSExtMapping(String ds_source, String ds_target, String ext_target) throws StorageException {
		IndexDescription index = IndexDescription.DSDTETXESX;
		return  getIndexStorage(index).getDataSet(index, DataField.E_SOURCE_EXT, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_TARGET_EXT, ext_target));
	}
}
