package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.storage.AbstractDataStorage;
import edu.unika.aifb.graphindex.storage.DataStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneDataStorage extends AbstractDataStorage {

	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	public LuceneDataStorage(String directory) {
		m_directory = directory;	
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		super.initialize(clean, readonly);
		try {
			if (!m_readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(256);
			}
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		}
		catch (CorruptIndexException e) {
			throw new StorageException(e);
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void close() throws StorageException {
		try {
			if (!m_readonly && m_writer != null)
				m_writer.close();
			if (m_searcher != null)
				m_searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public void optimize() throws StorageException {
		if (!m_readonly) {
			try {
				m_writer.optimize();
			} catch (CorruptIndexException e) {
				throw new StorageException(e);
			} catch (IOException e) {
				throw new StorageException(e);
			}
		}
	}

	private void reopen() throws CorruptIndexException, IOException {
		m_reader = m_reader.reopen();
		m_searcher = new IndexSearcher(m_reader);
	}
		
	private Document createDocument(String src, String edge, String dst, String type) {
		Document doc = new Document();
		doc.add(new Field(SRC_FIELD, src, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(EDGE_FIELD, edge, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(DST_FIELD, dst, Field.Store.YES, Field.Index.UN_TOKENIZED));
		doc.add(new Field(TYPE_FIELD, type, Field.Store.YES, Field.Index.UN_TOKENIZED));
		return doc;
	}

	public void addTriple(String src, String edge, String dst, String type) throws StorageException {
		try {
			Document doc = createDocument(src, edge, dst, type);
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	public IndexSearcher getIndexSearcher() {
		return m_searcher;
	}

}
