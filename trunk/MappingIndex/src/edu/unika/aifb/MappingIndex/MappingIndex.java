package edu.unika.aifb.MappingIndex;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.*;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;

public class MappingIndex extends Index {
	
	// IndexDescription and IndexStorage mapping
	private Map<IndexDescription,IndexStorage> m_indexes;
	// Index directory
	private String m_root_dir;

	public MappingIndex(String idxDirectory, IndexConfiguration idxConfig) throws IOException, StorageException {
		super(new IndexDirectory(idxDirectory), idxConfig);
		m_indexes = new HashMap<IndexDescription, IndexStorage>();
		m_root_dir = idxDirectory;
		openAllIndexes();
	}
	
	/**
	 * Closes all opened index storages.
	 */
	public void close() throws StorageException {
		for (IndexStorage is : m_indexes.values())
			is.close();
		m_indexes.clear();
	}
	
	/**
	 * Opens all indexes listed in the index configuration.
	 * @throws StorageException
	 */
	private void openAllIndexes() throws StorageException {
		for (IndexDescription index : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES))
			getIndexStorage(index);
	}
	
	/**
	 * Returns the index storage for the given index description.
	 * @param index
	 * @return
	 * @throws StorageException
	 */
	public IndexStorage getIndexStorage(IndexDescription index) throws StorageException {
		IndexStorage is = m_indexes.get(index);
		if (is == null) {
			is = new LuceneIndexStorage(new File(new File(m_root_dir + "/mp").getAbsolutePath() + "/" + index.getIndexFieldName()), m_idxReader != null ? m_idxReader.getCollector() : new StatisticsCollector());
			is.initialize(false, true);				
			m_indexes.put(index, is);
		}
		return is;
	}
	
	/**
	 * 
	 * @param ds_source
	 * @param ds_target
	 * @param e_source
	 * @return
	 * @throws StorageException
	 */
	public Table<String> getStoTMapping(String ds_source, String ds_target, String e_source) throws StorageException {
		IndexDescription index = IndexDescription.DSDTESET;
		return  getIndexStorage(index).getTable(index, new DataField[] { DataField.E_SOURCE, DataField.E_TARGET }, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_SOURCE, e_source));
	}
	
	public Table<String> getStoTMapping(String ds_source, String ds_target) throws StorageException {
		IndexDescription index = IndexDescription.DSDTESET;
		return  getIndexStorage(index).getTable(index, new DataField[] { DataField.E_SOURCE, DataField.E_TARGET }, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target));
	}
	
	/**
	 * 
	 * @param ds_source
	 * @param ds_target
	 * @param e_target
	 * @return
	 * @throws StorageException
	 */
	public Table<String> getTtoSMapping(String ds_source, String ds_target, String e_target) throws StorageException {
		IndexDescription index = IndexDescription.DSDTETES;
		return  getIndexStorage(index).getTable(index, new DataField[] { DataField.E_TARGET, DataField.E_SOURCE }, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_TARGET, e_target));
	}
	
	/**
	 * 
	 * @param ds_source
	 * @param ds_target
	 * @param ext_source
	 * @return
	 * @throws StorageException
	 */
	public Table<String> getStoTExtMapping(String ds_source, String ds_target, String ext_source) throws StorageException {
		IndexDescription index = IndexDescription.DSDTESXETX;
		return  getIndexStorage(index).getTable(index, new DataField[] {DataField.E_SOURCE_EXT, DataField.E_TARGET_EXT }, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_SOURCE_EXT, ext_source));
	}
	
	/**
	 * 
	 * @param ds_source
	 * @param ds_target
	 * @param ext_target
	 * @return
	 * @throws StorageException
	 */
	public Table<String> getTtoSExtMapping(String ds_source, String ds_target, String ext_target) throws StorageException {
		IndexDescription index = IndexDescription.DSDTETXESX;
		return  getIndexStorage(index).getTable(index, new DataField[] { DataField.E_TARGET_EXT, DataField.E_SOURCE_EXT }, 
				index.createValueArray(DataField.DS_SOURCE, ds_source, DataField.DS_TARGET, ds_target, DataField.E_TARGET_EXT, ext_target));
	}
}
