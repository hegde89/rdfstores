import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;


public class MappingIndexCreator implements TripleSink{
	
	private IndexDirectory m_idxDirectory;
	private IndexConfiguration m_idxConfig;
	private Importer m_importer;
	
	private Map<IndexDescription,IndexStorage> m_dataIndexes;
	
	private Set<String> m_properties;
	private String m_ds_source;
	private String m_ds_destination;
	
	public MappingIndexCreator(IndexDirectory indexDirectory, String s, String d) throws IOException {
		m_idxDirectory = indexDirectory;
		m_idxConfig = new IndexConfiguration();
		m_properties = new HashSet<String>();
		m_ds_source = s;
		m_ds_destination = d;
	}
	
	public void setImporter(Importer importer) {
		m_importer = importer;
	}
	
	public void setCreateDataIndex(boolean createDI) {
		m_idxConfig.set(IndexConfiguration.HAS_DI, createDI);
	}
	
	private void addDataIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.DI_INDEXES, idx);
	}
	
	private void importData() throws IOException, StorageException {
		m_importer.setTripleSink(this);
		
		m_dataIndexes = new HashMap<IndexDescription,IndexStorage>();
		
		IndexStorage is = new LuceneIndexStorage(new File(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR, true).getAbsolutePath() + "/" + IndexDescription.DSEEDS.getIndexFieldName()), new StatisticsCollector());
		is.initialize(true, false);
		m_dataIndexes.put(IndexDescription.DSEEDS, is);
		
		m_importer.doImport();
		
		Util.writeEdgeSet(m_idxDirectory.getFile(IndexDirectory.PROPERTIES_FILE, true), m_properties);

		m_dataIndexes.get(IndexDescription.DSEEDS).mergeSingleIndex(IndexDescription.DSEEDS);
		m_dataIndexes.get(IndexDescription.DSEEDS).close();
	}
	
	public void create() throws FileNotFoundException, IOException, StorageException, InterruptedException {
		m_idxDirectory.create();

		addDataIndex(IndexDescription.DSEEDS);
		
		importData();
	}
	
	private String selectByField(DataField df, String s, String p, String o, String c) {
		if (df == DataField.SUBJECT)
			return s;
		else if (df == DataField.PROPERTY)
			return p;
		else if (df == DataField.OBJECT)
			return o;
		else if (df == DataField.CONTEXT)
			return c;
		else
			return null;
	}
	
	public void triple(String s, String p, String o, String c) {
		m_properties.add(p);
		
		if (c == null) c = "";
		
		IndexDescription idx = IndexDescription.DSEEDS;
		String[] indexFields = new String [4];
		
		indexFields[0] = m_ds_source;
		indexFields[1] = s;
		indexFields[2] = o;
		indexFields[3] = m_ds_destination;
		
		String value = selectByField(idx.getValueField(), s, p, o, c);
		
		if (value == null) {
			throw new UnsupportedOperationException("data indexes can only consist of S, P and O data fields");
		}
		
		try {
			m_dataIndexes.get(idx).addData(idx, indexFields, value);
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}
}
