package edu.unika.aifb.graphindex.storage.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.TermStorage;
import edu.unika.aifb.graphindex.util.Timings;

public class LuceneTermStorage implements TermStorage {

	private File m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	private long m_nextId;
	private LRUCache<Integer,Document> m_docCache;
	
	private final String FIELD_TERM = "term";
	private final String FIELD_ID = "id";

	public LuceneTermStorage(File directory) {
		m_directory = directory;
	}
	
	@Override
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		try {
			Directory dir = FSDirectory.getDirectory(m_directory);
			if (!readonly) 
				m_writer = new IndexWriter(dir, new WhitespaceAnalyzer(), clean, MaxFieldLength.UNLIMITED);
			m_reader = IndexReader.open(dir);
			m_searcher = new IndexSearcher(m_reader);
			
			m_nextId = m_reader.numDocs() + 1;
			m_docCache = new LRUCache<Integer,Document>(5000);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (LockObtainFailedException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public void close() throws StorageException {
		try {
			m_searcher.close();
			m_reader.close();
			if (m_writer != null)
				m_writer.close();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	@Override
	public Long add(String term) throws StorageException {
		long id = ++m_nextId;
		
		Document doc = new Document();
		doc.add(new Field(FIELD_TERM, term, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
		doc.add(new Field(FIELD_ID, new String(Longs.toByteArray(id), Charsets.UTF_8), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
		
		try {
			m_writer.addDocument(doc);
			m_writer.commit();
			IndexReader r = m_reader.reopen();
			if (r != m_reader) {
				m_reader.close();
				m_searcher.close();
				m_searcher = new IndexSearcher(r);
			}
			m_reader = r;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return id;
	}

	public List<Integer> getDocumentIds(Query q) throws StorageException {
		final List<Integer> docIds = new ArrayList<Integer>();
		try {
			m_searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					docIds.add(docId);
				}
			});
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		Collections.sort(docIds);
		
		return docIds;
	}

	private Document getDocument(int docId) throws StorageException {
		try {
			Document doc = m_docCache.get(docId);
			if (doc == null) {
				doc = m_reader.document(docId);
				m_docCache.put(docId, doc);
			}
			return doc;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public Long getId(String term) throws StorageException {
		TermQuery q = new TermQuery(new Term(FIELD_TERM, term));
		List<Integer> docIds = getDocumentIds(q);
		if (docIds.size() != 1)
			return null;
		Document doc = getDocument(docIds.get(0));
		return Longs.fromByteArray(doc.get(FIELD_ID).getBytes(Charsets.UTF_8));
	}

	@Override
	public String getTerm(long id) throws StorageException {
		TermQuery q = new TermQuery(new Term(FIELD_ID, new String(Longs.toByteArray(id), Charsets.UTF_8)));
		List<Integer> docIds = getDocumentIds(q);
		if (docIds.size() != 1)
			return null;
		Document doc = getDocument(docIds.get(0));
		return doc.get(FIELD_TERM);
	}
}
