package edu.unika.aifb.MappingIndex;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.index.*;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;


public class MappingIndexCreator implements TripleSink{
	
	private IndexDirectory m_idxDirectory;
	private IndexConfiguration m_idxConfig;
	private Importer m_importer;
	private int m_triplesImported = 0;
	private final int TRIPLES_INTERVAL = 500000;
	
	private Map<IndexDescription,IndexStorage> m_mappingIndexes;
	
	// Source data source
	private String m_ds_source;
	// Target data source
	private String m_ds_destination;
	
	// Structure Index target data source
	private StructureIndex o_idx;
	// Structure Index source data source
	private StructureIndex s_idx;
	
	private final static Logger log = Logger.getLogger(IndexCreator.class);
	
	public MappingIndexCreator(IndexDirectory indexDirectory, String s, String d) throws IOException {
		m_idxDirectory = indexDirectory;
		m_idxConfig = new IndexConfiguration();
		m_ds_source = s;
		m_ds_destination = d;
	}
	
	public void setImporter(Importer importer) {
		m_importer = importer;
	}
	
	private void addMappingIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.DI_INDEXES, idx);
	}
	
	private void addSPMappingIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.SP_INDEXES, idx);
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

		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			log.debug("merging " + idx.toString());
			m_mappingIndexes.get(idx).mergeSingleIndex(idx);
			m_mappingIndexes.get(idx).close();
		}
	}
	
	/**
	 * Creates the index directory for the new indices and opens the structure index of the source data source and
	 * the target data source.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws StorageException
	 * @throws InterruptedException
	 */
	public void create() throws FileNotFoundException, IOException, StorageException, InterruptedException {
		// Create directory for indices
		m_idxDirectory.create();
		
		// ds1,ds2,e1->e2
		addMappingIndex(IndexDescription.DSDTESET);
		// ds1,ds2,e2->e1
		addMappingIndex(IndexDescription.DSDTETES);
		// ds1,ds2,e1_ext -> e2_ext
		addMappingIndex(IndexDescription.DSDTESXETX);
		// ds1,ds2,e2_ext -> e1_ext
		addMappingIndex(IndexDescription.DSDTETXESX);
		
		// Open index of target entity
		//IndexReader o_IndexReader = new IndexReader(new IndexDirectory(m_ds_destination));
		//o_idx = o_IndexReader.getStructureIndex();
		
		// Open index of source entity
		//IndexReader s_IndexReader = new IndexReader(new IndexDirectory(m_ds_source));
		//s_idx = s_IndexReader.getStructureIndex();
		
		// Import triples from mapping file
		//importData();
		
		m_idxConfig.store(m_idxDirectory);		
	}
	
	/**
	 * This method is called by the importer as triple sink to process the triples.
	 * @param s Subject
	 * @param p Property
	 * @param o Object
	 * @param c Context
	 */
	public void triple(String s, String p, String o, String c) {		

		try {
			// Get object extension
			String objExt = o_idx.getExtension(o);
			
			// Get subject extension
			String subExt = s_idx.getExtension(s);
			
			// Entity mapping index
			m_mappingIndexes.get(IndexDescription.DSDTESET).addData(IndexDescription.DSDTESET, new String[] { m_ds_source, m_ds_destination, s}, o);
			m_mappingIndexes.get(IndexDescription.DSDTETES).addData(IndexDescription.DSDTETES, new String[] { m_ds_source, m_ds_destination, o}, s);	
			
			// Extension mapping index
			m_mappingIndexes.get(IndexDescription.DSDTESXETX).addData(IndexDescription.DSDTESXETX, new String[] { m_ds_source, m_ds_destination, subExt}, objExt);
			m_mappingIndexes.get(IndexDescription.DSDTETXESX).addData(IndexDescription.DSDTETXESX, new String[] { m_ds_source, m_ds_destination, objExt}, subExt);
		
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Count imported triples	
		m_triplesImported++;
		if (m_triplesImported % TRIPLES_INTERVAL == 0)
			log.info("triples imported: " + m_triplesImported);
	}
}
