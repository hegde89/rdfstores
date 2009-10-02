package edu.unika.aifb.MappingIndex;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	private String m_ds_source;
	// Target data source
	private String m_ds_destination;
	// Mapping Directory
	private String m_mapping_dir;
	// Output Directory
	private String m_root_dir;

	public MappingIndex(String idxDirectory, IndexConfiguration idxConfig, String source, String target) throws IOException, StorageException {
		super(new IndexDirectory(idxDirectory), idxConfig);
		m_indexes = new HashMap<IndexDescription, IndexStorage>();
		m_ds_source = source;
		m_ds_destination = target;
		m_root_dir = idxDirectory;
		//openAllIndexes();
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
			try {
				// Get directory name for this mapping out of the name of both data sources
				m_mapping_dir = m_ds_source.replaceAll("[_[^\\w\\d]]", "") + "_" + m_ds_destination.replaceAll("[_[^\\w\\d]]", "");
				
				is = new LuceneIndexStorage(new File(getDirectory(m_mapping_dir, false).getAbsolutePath() + "/" + index.getIndexFieldName()), m_idxReader != null ? m_idxReader.getCollector() : new StatisticsCollector());
				is.initialize(false, true);
				//((LuceneIndexStorage)is).warmup(index, Util.readEdgeSet(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR).getAbsolutePath() + "/" + index.getIndexFieldName() + "_warmup", false));
				m_indexes.put(index, is);
			} catch (IOException e) {
				throw new StorageException(e);
			}
		}
		return is;
	}
	
	public Iterator<String[]> iterator(String valueField) throws StorageException {
		//IndexDescription index = getSuitableIndex(DataField.PROPERTY);
		IndexDescription index = IndexDescription.DSDTESET;
		return getIndexStorage(index).iterator(index, new DataField[] { DataField.DS_SOURCE, DataField.DS_TARGET, DataField.E_SOURCE, DataField.E_TARGET }, valueField);
	}
	
	private File getDirectory(String dir, boolean empty) throws IOException {
		String directory = m_root_dir + "/" + dir;
		
		if (empty) {
			File f = new File(directory);
			if (!f.exists())
				f.mkdirs();
			else
				emptyDirectory(f);
		}
		
		return new File(directory);
	}
	
	private void emptyDirectory(File dir) {
		for (File f : dir.listFiles()) {
			if (f.isDirectory())
				emptyDirectory(f);
			else
				f.delete();
		}
	}
	
}
