package edu.unika.aifb.graphindex.storage.lucene;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;

import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.keywordsearch.Constant;
import edu.unika.aifb.keywordsearch.index.BloomFilter;

public class LuceneNeighborhoodStorage implements NeighborhoodStorage {

	private IndexReader m_reader;
	private IndexSearcher m_searcher;
//	private LRUCache<Integer,Document> m_docCache;

	public LuceneNeighborhoodStorage(String directory) throws StorageException {
		try {
			m_reader = IndexReader.open(directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public BloomFilter getNeighborhoodBloomFilter(String uri) throws StorageException {
		BloomFilter bf = null;
		TermQuery q = new TermQuery(new Term(Constant.URI_FIELD, uri));
		final List<Integer> docIds = new ArrayList<Integer>();
		try {
			m_searcher.search(q, new HitCollector() {
				@Override
				public void collect(int doc, float score) {
					docIds.add(doc);
				}
			});

			assert docIds.size() <= 1;
			
			if (docIds.size() == 0)
				return null;
			
			Document doc = getDocument(docIds.get(0));
			byte[] bytes = doc.getField(Constant.NEIGHBORHOOD_FIELD).binaryValue();
			
			ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(bytes);
			ObjectInputStream objectInput = new ObjectInputStream(byteArrayInput);
			bf = (BloomFilter)objectInput.readObject();
		} catch (IOException e) {
			throw new StorageException(e);
		} catch (ClassNotFoundException e) {
			throw new StorageException(e);
		}
		
		return bf;
	}

	private Document getDocument(int docId) throws StorageException {
		try {
			return m_reader.document(docId);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void close() throws StorageException {
		try {
			m_searcher.close();
			m_reader.close();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
}
