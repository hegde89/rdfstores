package edu.unika.aifb.graphindex.index;

import java.io.IOException;
import java.util.HashMap;

import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.TermStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneTermStorage;

public class TermIndex extends Index {
	private TermStorage m_ts;
	
	public TermIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig) throws IOException, StorageException {
		super(idxDirectory, idxConfig);
		init(true);
	}

	public TermIndex(IndexReader reader) throws IOException, StorageException {
		super(reader);
		init(false);
	}
	
	private void init(boolean clean) throws IOException, StorageException {
		m_ts = new LuceneTermStorage(m_idxDirectory.getDirectory(IndexDirectory.TERMS_DIR));
		m_ts.initialize(clean, false);
	}

	@Override
	public void close() throws StorageException {
		m_ts.close();
	}
	
	public long getId(String term) throws StorageException {
		Long id = m_ts.getId(term);
		if (id == null)
			id = m_ts.add(term);
		return id;
	}
	
	public String getTerm(long id) throws StorageException {
		return m_ts.getTerm(id);
	}
}
