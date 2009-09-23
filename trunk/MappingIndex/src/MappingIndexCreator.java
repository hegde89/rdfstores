import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.algorithm.largercp.LargeRCP;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneWarmer;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;


public class MappingIndexCreator implements TripleSink{
	
	private IndexDirectory m_idxDirectory;
	private IndexConfiguration m_idxConfig;
	private Importer m_importer;
	private int m_triplesImported = 0;
	private final int TRIPLES_INTERVAL = 500000;
	
	private Map<IndexDescription,IndexStorage> m_mappingIndexes;
	
	private Set<String> m_properties;
	private String m_ds_source;
	private String m_ds_destination;
	
	private final static Logger log = Logger.getLogger(IndexCreator.class);
	
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
	
	private void addMappingIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.DI_INDEXES, idx);
	}
	
	
	private void importData() throws IOException, StorageException {
		m_importer.setTripleSink(this);
		
		m_mappingIndexes = new HashMap<IndexDescription,IndexStorage>();
		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			IndexStorage is = new LuceneIndexStorage(new File(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR, true).getAbsolutePath() + "/" + idx.getIndexFieldName()), new StatisticsCollector());
			is.initialize(true, false);
			m_mappingIndexes.put(idx, is);
		}
		
		m_importer.doImport();
		
		//Util.writeEdgeSet(m_idxDirectory.getFile(IndexDirectory.PROPERTIES_FILE, true), m_properties);

		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			log.debug("merging " + idx.toString());
			m_mappingIndexes.get(idx).mergeSingleIndex(idx);
			m_mappingIndexes.get(idx).close();
			
			//Util.writeEdgeSet(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR, false) + "/" + idx.getIndexFieldName() + "_warmup", 
			//	LuceneWarmer.getWarmupTerms(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR, false) + "/" + idx.getIndexFieldName(), 10));
		}
	}
	
	public void create() throws FileNotFoundException, IOException, StorageException, InterruptedException {
		m_idxDirectory.create();

		addMappingIndex(IndexDescription.DSDTESET);
		addMappingIndex(IndexDescription.DSDTETES);
		importData();
	}
	
	
	public void triple(String s, String p, String o, String c) {		
			try {
				m_mappingIndexes.get(IndexDescription.DSDTESET).addData(IndexDescription.DSDTESET, new String[] { m_ds_source, m_ds_destination, s}, o);
				m_mappingIndexes.get(IndexDescription.DSDTETES).addData(IndexDescription.DSDTETES, new String[] { m_ds_source, m_ds_destination, o}, s);
			} catch (StorageException e) {
				e.printStackTrace();
			}
		
		m_triplesImported++;
		if (m_triplesImported % TRIPLES_INTERVAL == 0)
			log.info("triples imported: " + m_triplesImported);
	}
}
