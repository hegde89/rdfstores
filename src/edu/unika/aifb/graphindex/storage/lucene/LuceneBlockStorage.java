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

import edu.unika.aifb.graphindex.storage.AbstractBlockStorage;
import edu.unika.aifb.graphindex.storage.AbstractDataStorage;
import edu.unika.aifb.graphindex.storage.DataStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneBlockStorage extends AbstractBlockStorage {

	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	public LuceneBlockStorage(String directory) {
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
		
	private Document createDocument(String block, String element) {
		Document doc = new Document();
		doc.add(new Field(BLOCK_FIELD, block, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(ELE_FIELD, element, Field.Store.YES, Field.Index.UN_TOKENIZED));
		if(element.startsWith("http://"))
			doc.add(new Field(LIT_FIELD, "false", Field.Store.YES, Field.Index.NO));
		else
			doc.add(new Field(LIT_FIELD, "true", Field.Store.YES, Field.Index.NO));
		return doc;
	}

	public void addBlock(String block, String element) throws StorageException {
		try {
			Document doc = createDocument(block, element);
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
