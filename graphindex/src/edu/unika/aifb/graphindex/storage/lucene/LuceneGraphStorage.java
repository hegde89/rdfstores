package edu.unika.aifb.graphindex.storage.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import edu.unika.aifb.graphindex.algorithm.largercp.Block;
import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.storage.AbstractGraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.Util;

public class LuceneGraphStorage extends AbstractGraphStorage {

	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	private boolean m_storeGraphName = true;
	private LRUCache<Integer,Document> m_docCache = new LRUCache<Integer,Document>(5000000);
	private LRUCache<String,Boolean> m_imageZeroCache = new LRUCache<String,Boolean>(5000000);
	private LRUCache<String,Boolean> m_preimageZeroCache = new LRUCache<String,Boolean>(5000000);
	
	public static final String FIELD_GRAPH = "graph";
	public static final String FIELD_SRC = "src";
	public static final String FIELD_EDGE = "edge";
	public static final String FIELD_DST = "dst";
	public static final String FIELD_TYPE = "type"; 
	public int m_docCacheMisses;
	public int m_docCacheHits;
	
	public LuceneGraphStorage(String directory) {
		m_directory = directory;
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		super.initialize(clean, readonly);
		try {
			if (!m_readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(4096);
				m_writer.setMergeFactor(50);
			}
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		}
		catch (CorruptIndexException e) {
			throw new StorageException(e);
		}
		catch (FileNotFoundException e) {
			try {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), true);
				m_writer.setRAMBufferSizeMB(1024);
			} catch (CorruptIndexException e1) {
				e1.printStackTrace();
			} catch (LockObtainFailedException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
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
	
	public void setStoreGraphName(boolean storeGraphName) {
		m_storeGraphName = storeGraphName;
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
		if (m_storeGraphName)
			doc.add(new Field(FIELD_GRAPH, graphName, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_SRC, source, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_EDGE, edge, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_DST, target, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
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
//		log.debug("  " + docIds.size() + " docs");
		
		Collections.sort(docIds);
		
		return docIds;
	}
	
	private Document getDocument(int docId) throws StorageException {
		try {
			Document doc = m_docCache.get(docId);
			if (doc == null) {
				m_docCacheMisses++;
				doc = m_reader.document(docId);
				m_docCache.put(docId, doc);
			}
			else
				m_docCacheHits++;
			return doc;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public Map<String,Set<String>> getImage(String node, boolean preimage) throws StorageException {
		Map<String,Set<String>> image = new HashMap<String,Set<String>>();
		
		String idxField = preimage ? FIELD_DST : FIELD_SRC;
		String dataField = preimage ? FIELD_SRC : FIELD_DST;
		
		Query q = new TermQuery(new Term(idxField, node));
		List<Integer> docIds = getDocumentIds(q);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			String p = doc.getField(FIELD_EDGE).stringValue();
			String d = doc.getField(dataField).stringValue();
			
			Set<String> data = image.get(p);
			if (data == null) {
				data = new HashSet<String>();
				image.put(p, data);
			}
			data.add(d);
		}
		
		return image;
	}
	
	public Set<String> getImage(String node, String property, boolean preimage) throws StorageException {
		String key = node + "__" + property;
		Boolean isEmpty = preimage ? m_preimageZeroCache.get(key) : m_imageZeroCache.get(key);
		if (isEmpty != null && isEmpty.booleanValue() == true)
			return new HashSet<String>();
		
		Set<String> image = new HashSet<String>(5000);
		String idxField = preimage ? FIELD_DST : FIELD_SRC;
		String dataField = preimage ? FIELD_SRC : FIELD_DST;
		
		BooleanQuery bq = new BooleanQuery();
		bq.add(new TermQuery(new Term(idxField, node)), Occur.MUST);
		bq.add(new TermQuery(new Term(FIELD_EDGE, property)), Occur.MUST);
		
		List<Integer> docIds = getDocumentIds(bq);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			String d = doc.getField(dataField).stringValue();
			image.add(d);
		}
		
		Boolean val = image.size() == 0;
		if (preimage)
			m_preimageZeroCache.put(key, val);
		else
			m_imageZeroCache.put(key, val);
		
		return image;
	}
	
	public Set<String> getEdges() throws StorageException {
		Set<String> edges = new HashSet<String>();
		try {
			TermEnum te = m_reader.terms(new Term(FIELD_EDGE, ""));
			do {
				Term t = te.term();
				String field = t.field();
				if (field.equals(FIELD_EDGE))
					edges.add(t.text());
				else
					break;
			}
			while (te.next());
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return edges;
	}
	
	public Set<String> getNodes() throws StorageException {
		Set<String> nodes = new HashSet<String>();
		try {
			TermEnum te = m_reader.terms();
			while (te.next()){
				Term t = te.term();
				String field = t.field();
				if (field.equals(FIELD_DST) || field.equals(FIELD_SRC))
					nodes.add(t.text());
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return nodes;
	}
	
	public boolean hasEntityNodes(int pos, String property) throws StorageException {
		String f = pos == 0 ? FIELD_SRC : FIELD_DST;
		Query q = new TermQuery(new Term(FIELD_EDGE, property));
		List<Integer> docIds = getDocumentIds(q);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			String val = doc.getField(f).stringValue();
			if (Util.isEntity(val))
				return true;
		}
		return false;
 	}

	public Set<String> getNodes(int pos) throws StorageException {
		Set<String> nodes = new HashSet<String>();
		String f = pos == 0 ? FIELD_SRC : FIELD_DST;
		try {
			TermEnum te = m_reader.terms(new Term(f, ""));
			do {
				Term t = te.term();
				String field = t.field();
				if (field.equals(f))
					nodes.add(t.text());
				else
					break;
			}
			while (te.next());
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return nodes;
	}
	
	public Set<String> getNodes(int pos, String property) throws StorageException {
		return getNodes(pos, property, false);
	}
	
	public Set<String> getNodes(int pos, String property, boolean ignoreDataValues) throws StorageException {
		Set<String> nodes = new HashSet<String>();
		String f = pos == 0 ? FIELD_SRC : FIELD_DST;

		Query q = new TermQuery(new Term(FIELD_EDGE, property));
		List<Integer> docIds = getDocumentIds(q);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			String val = doc.getField(f).stringValue();
			if (!ignoreDataValues || Util.isEntity(val))
				nodes.add(val);
		}
		
		return nodes;
	}

	public void addNodesToBC(BlockCache bc, Block block, boolean ignoreDataValues) throws StorageException {
		try {
			TermEnum te = m_reader.terms();
			while (te.next()) {
				Term t = te.term();
				String field = t.field();
				if ((field.equals(FIELD_DST) || field.equals(FIELD_SRC)) && (!ignoreDataValues || Util.isEntity(t.text())))
					bc.setBlock(t.text(), block);
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
	}

	public IndexSearcher getIndexSearcher() {
		return m_searcher;
	}

	public void addEdge(String graphName, String source, String edge, String target, String type) throws StorageException {
		try {
			Document doc = createDocument(graphName, source, edge, target, type);
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
	}

	private Document createDocument(String graphName, String source, String edge, String target, String type) {
		Document doc = new Document();
		if (m_storeGraphName)
			doc.add(new Field(FIELD_GRAPH, graphName, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_SRC, source, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_EDGE, edge, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_DST, target, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_TYPE, type, Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		return doc;
	}
	
}
