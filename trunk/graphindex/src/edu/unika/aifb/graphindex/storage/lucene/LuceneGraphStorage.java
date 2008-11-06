package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.storage.AbstractGraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneGraphStorage extends AbstractGraphStorage {

	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	private final String FIELD_GRAPH = "graph";
	private final String FIELD_SRC = "src";
	private final String FIELD_EDGE = "edge";
	private final String FIELD_DST = "dst";
	
	public LuceneGraphStorage(String directory) {
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
	
	public Set<String> loadGraphList() throws StorageException {
		Set<String> names = new HashSet<String>();
		try {
			TermEnum te = m_reader.terms();
			while (te.next()) {
				Term t = te.term();
				if (t.field().equals(FIELD_GRAPH)) {
					names.add(t.text());
				}
			}
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return names;
	}

	public void saveGraphList(Set<String> graphs) throws StorageException {
		// no need here, graph list is implicit in graph field of the documents
	}

	public Set<LabeledEdge<String>> loadEdges(String graphName) throws StorageException {
		Query q = new TermQuery(new Term(FIELD_GRAPH, graphName));
		
		Set<LabeledEdge<String>> edges = new HashSet<LabeledEdge<String>>();

		// TODO lucene doc says that retrieving all documents matching a query should be done with HitCollector
		try {
			Hits hits = m_searcher.search(q);
			
			for (Iterator i = hits.iterator(); i.hasNext(); ) {
				Hit hit = (Hit)i.next();
				Document doc = hit.getDocument();
				LabeledEdge<String> e = new LabeledEdge<String>(doc.getField(FIELD_SRC).stringValue(), doc.getField(FIELD_DST).stringValue(), doc.getField(FIELD_EDGE).stringValue());
				edges.add(e);
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return edges;
	}
	
	private Document createDocument(String graphName, String source, String edge, String target) {
		Document doc = new Document();
		doc.add(new Field(FIELD_GRAPH, graphName, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_SRC, source, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
		doc.add(new Field(FIELD_EDGE, edge, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
		doc.add(new Field(FIELD_DST, target, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
		return doc;
	}

	public void saveEdges(String graphName, Set<LabeledEdge<String>> edges) throws StorageException {
		try {
			m_writer.deleteDocuments(new Term(FIELD_GRAPH, graphName));
			for (LabeledEdge<String> edge : edges) {
				Document doc = createDocument(graphName, edge.getSrc(), edge.getLabel(), edge.getDst());
				m_writer.addDocument(doc);
			}
			
			m_writer.flush();
			reopen();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addEdge(String graphName, String source, String edge, String target) throws StorageException {
		try {
			Document doc = createDocument(graphName, source, edge, target);
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
}
