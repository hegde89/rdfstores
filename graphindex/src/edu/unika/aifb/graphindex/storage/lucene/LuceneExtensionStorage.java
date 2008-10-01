package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.data.ExtensionSegment;
import edu.unika.aifb.graphindex.data.ListExtensionSegment;
import edu.unika.aifb.graphindex.data.SetExtensionSegment;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.storage.AbstractExtensionStorage;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneExtensionStorage extends AbstractExtensionStorage {
	
	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	private LRUCache<Integer,Document> m_docCache;
	
	private final String FIELD_EXT = "ext";
	private final String FIELD_SUBJECT = "subject";
	private final String FIELD_PROPERTY = "property";
	private final String FIELD_OBJECT = "object";
	private final String FIELD_TYPE = "type";
	private final String FIELD_VAL = "value";
	private final String TYPE_EXTLIST = "extension_list";
	private final String EXT_PATH_SEP = "__";
	private final static Logger log = Logger.getLogger(LuceneExtensionStorage.class);
	
	public LuceneExtensionStorage(String directory) {
		m_directory = directory;
		m_docCache = new LRUCache<Integer,Document>(1024);
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		super.initialize(clean, readonly);
		try {
			if (!m_readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(1024);
			}
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		BooleanQuery.setMaxClauseCount(65536);
	}
	
	public void close() {
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
	
	private void checkReadOnly() {
		if (m_readonly)
			throw new UnsupportedOperationException("no updates while readonly");
	}
	
	public void startBulkUpdate() throws StorageException {
		checkReadOnly();
	}
	
	public void finishBulkUpdate() throws StorageException {
		try {
			log.debug("flushing...");
			if (!m_readonly)
				m_writer.flush();
			reopen();
			log.debug("flushed and reopened");
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public Set<String> loadExtensionList() throws StorageException {
		if (bulkUpdating())
			throw new StorageException("in bulk mode");
		
		Set<String> uris = new HashSet<String>();
		
		try {
			TermEnum te = m_reader.terms();
			while (te.next()) {
				Term t = te.term();
				if (t.field().equals(FIELD_EXT)) {
					String[] path = t.text().split(EXT_PATH_SEP.replaceAll("\\|", "\\\\|"));
					uris.add(path[0]);
				}
			}
			log.debug(uris.size() + " extensions");
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return uris;
	}
	
	public void saveExtensionList(Set<String> uris) throws StorageException {
		checkReadOnly();
		if (bulkUpdating())
			throw new StorageException("in bulk mode");
	}
	
	public void flushWriter() throws CorruptIndexException, IOException {
		if (!m_readonly)
			m_writer.flush();
		reopen();
	}
	
	private void reopen() throws CorruptIndexException, IOException {
		m_reader = m_reader.reopen();
		m_searcher = new IndexSearcher(m_reader);
	}
	
	private String getPath(String... args) {
		String path = "";
		for (String arg : args)
			path += arg + EXT_PATH_SEP;
		return path;
	}
	
	private Term getTerm(String extUri, String propertyUri, String object) {
		return getTermForPath(getPath(extUri, propertyUri, object));
	}
	
	private Term getTerm(String extUri, String propertyUri) {
		return getTermForPath(getPath(extUri, propertyUri));
	}
	
	private Term getTerm(String extUri) {
		return getTermForPath(getPath(extUri));
	}
	
	private Term getTermForPath(String path) {
		return new Term(FIELD_EXT, path);
	}
	
	@SuppressWarnings("unchecked")
	private Set<Triple> executeQuery(Query q) throws IOException {
		long start = System.currentTimeMillis();
//		log.debug(q.rewrite(m_reader));
		
		// TODO lucene doc says that retrieving all documents matching a query should be done with HitCollector
//		Hits hits = m_searcher.search(q);
//		for (Iterator i = hits.iterator(); i.hasNext(); ) {
//			Hit hit = (Hit)i.next();
//			Document doc = hit.getDocument();
//			Triple t = new Triple(doc.getField(FIELD_SUBJECT).stringValue(), doc.getField(FIELD_PROPERTY).stringValue(), doc.getField(FIELD_OBJECT).stringValue());
//			triples.add(t);
//		}
//		log.debug("searching " + q);
		final List<Integer> docIds = new ArrayList<Integer>();
		m_searcher.search(q, new HitCollector() {
			@Override
			public void collect(int docId, float score) {
				docIds.add(docId);
			}
		});
		long search = System.currentTimeMillis() - start;
		
//		Set<Triple> triples = new HashSet<Triple>(docIds.size());
		List<Triple> tripleList = new ArrayList<Triple>(docIds.size() + 1);
		Collections.sort(docIds);
		for (int docId : docIds) {
			Document doc = m_reader.document(docId);
			Triple t = new Triple(doc.getField(FIELD_SUBJECT).stringValue(), doc.getField(FIELD_PROPERTY).stringValue(), doc.getField(FIELD_OBJECT).stringValue());
//			triples.add(t);
			tripleList.add(t);
		}
		
		long retrieval = System.currentTimeMillis() - start - search;

		log.debug("query: " + q + " (" + docIds.size() + " results) {" + (System.currentTimeMillis() - start) + " ms, s: " + search + " ms, r: " + retrieval + " ms}");
		
		return new HashSet<Triple>(tripleList);
//		return triples;
	}
	
	private List<Document> getDocuments(Query q) throws CorruptIndexException, IOException {
		long start = System.currentTimeMillis();
		
		final List<Integer> docIds = new ArrayList<Integer>();
		m_searcher.search(q, new HitCollector() {
			public void collect(int docId, float score) {
				docIds.add(docId);
			}
		});
		
		long search = System.currentTimeMillis() - start;
		
		Collections.sort(docIds);
		List<Document> docs = new ArrayList<Document>(docIds.size());
		for (int docId : docIds) {
			Document doc = m_docCache.get(docId);
			if (doc == null) {
				doc = m_reader.document(docId);
				m_docCache.put(docId, doc);
			}
			docs.add(doc);
		}

		long retrieval = System.currentTimeMillis() - start - search;

//		log.debug("query: " + q + " (" + docIds.size() + " docs) {" + (System.currentTimeMillis() - start) + " ms, s: " + search + " ms, r: " + retrieval + " ms}");
		
		return docs;
	}
	
	public boolean hasDocs(String ext, String propertyUri, String object) throws StorageException  {
		try {
			long start = System.currentTimeMillis();
			Query q = new TermQuery(getTerm(ext, propertyUri, object));
			final List<Integer> docIds = new ArrayList<Integer>();
			m_searcher.search(q, new HitCollector() {
				public void collect(int doc, float score) {
					docIds.add(doc);
				}
			});
			log.debug("hasDocs q: " + q + " (" + docIds.size() + ") {" + (System.currentTimeMillis() - start) + " ms}");
			return docIds.size() > 0;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	private Document createDocument(String extUri, Triple t) {
		Document doc = new Document();
		doc.add(new Field(FIELD_EXT, getPath(extUri, t.getProperty(), t.getObject()), Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_SUBJECT, t.getSubject(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
		doc.add(new Field(FIELD_PROPERTY, t.getProperty(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
		doc.add(new Field(FIELD_OBJECT, t.getObject(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
//		log.debug(doc);
		
		return doc;
	}
	
	private String getPath(ExtensionSegment es) {
		return es.getExtensionUri() + EXT_PATH_SEP + es.getProperty() + EXT_PATH_SEP + es.getObject() + EXT_PATH_SEP;
	}
	
	private Document segmentToDocument(ExtensionSegment es) {
		Document doc = new Document();
		doc.add(new Field(FIELD_EXT, getPath(es), Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_OBJECT, es.getObject(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(FIELD_SUBJECT, es.toSubjectString(), Field.Store.YES, Field.Index.NO));
		return doc;
	}
	
	private ExtensionSegment documentToSegment(Document doc, String extUri, String propertyUri) {
		ListExtensionSegment es = new ListExtensionSegment(extUri, propertyUri, doc.getField(FIELD_OBJECT).stringValue());
		String[] subjects = doc.getField(FIELD_SUBJECT).stringValue().split("\n");
		es.setSubjects(Arrays.asList(subjects));
		return es;
	}
	
	public void saveExtensionSegment(ExtensionSegment es) throws CorruptIndexException, IOException {
		checkReadOnly();
		Document doc = segmentToDocument(es);
		m_writer.addDocument(doc);
		
		if (!bulkUpdating()) {
			m_writer.flush();
			reopen();
		}
	}
	
	private String[] getPathFromTerm(Term t) {
		String[] path = t.text().split(EXT_PATH_SEP);
		return path;
	}
	
	public List<ExtensionSegment> loadExtensionSegments(String extUri, String property) throws IOException {
		long start = System.currentTimeMillis();
		
		PrefixQuery pq = new PrefixQuery(getTerm(extUri, property));
		BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
		
		long rewrite = System.currentTimeMillis() - start;
		
		List<ExtensionSegment> segments = new ArrayList<ExtensionSegment>(bq.getClauses().length);
		for (BooleanClause clause : bq.getClauses()) {
			Term t = ((TermQuery)clause.getQuery()).getTerm();
			if (t.field().equals(FIELD_EXT)) {
				String[] path = getPathFromTerm(t);
				ExtensionSegment es = loadExtensionSegment(extUri, property, path[2]);
				if (es != null)
					segments.add(es);
			}
		}
		
		long duration = System.currentTimeMillis() - start;
		
//		log.debug(" duration: " + duration + ", rewrite: " + rewrite);
		return segments;
	}
	
	public ExtensionSegment loadExtensionSegment(String extUri, String property, String object) throws CorruptIndexException, IOException {
		long start = System.currentTimeMillis();
		
		TermQuery tq = new TermQuery(getTerm(extUri, property, object));
		List<Document> docs = getDocuments(tq);
		
		long dr = System.currentTimeMillis() - start;
		
		ExtensionSegment es;
		if (docs.size() == 1)
			es = documentToSegment(docs.get(0), extUri, property);
		else
			es = null;
		
		long build = System.currentTimeMillis() - start - dr;
		if (docs.size() > 0)
			log.debug("q: " + tq + " (" + docs.size() + " docs" + (es != null ? ", " + es.getSubjects().size() + " triples" : "") + ") {" + (System.currentTimeMillis() - start) + " ms, dr: " + dr + ", b: " + build + "}");
		
		return es;
	}
	
	public void mergeExtensions() throws IOException {
		TermEnum te = m_reader.terms();
		while (te.next()) {
			if (te.docFreq() > 1) {
				Term t = te.term();
//				log.debug("term " + t + " docfreq > 1");
				String[] path = getPathFromTerm(t);
				List<Document> docs = getDocuments(new TermQuery(t));
				Set<String> subjects = new HashSet<String>();
				for (Document doc : docs) {
					for (String subject : doc.getField(FIELD_SUBJECT).stringValue().split("\n"))
						subjects.add(subject);
				}
				SetExtensionSegment es = new SetExtensionSegment(path[0], path[1], path[2]);
				es.setSubjects(subjects);
				m_writer.updateDocument(t, segmentToDocument(es));
			}
		}
		log.debug("optimizing...");
		m_writer.optimize();
	}
	
	private void deleteDataByPath(String queryPath) throws IOException {
		List<Term> terms = new ArrayList<Term>();
		
		PrefixQuery q = new PrefixQuery(getTermForPath(queryPath));
		BooleanQuery bq = (BooleanQuery)q.rewrite(m_reader);
		
		for (BooleanClause bc : bq.getClauses())
			terms.add(((TermQuery)bc.getQuery()).getTerm());
		log.debug("delete: " + queryPath);
		m_writer.deleteDocuments(terms.toArray(new Term[]{}));
		if (!bulkUpdating()) {
			m_writer.flush();
			reopen();
		}
	}
	
	public void deleteData(String extUri) throws IOException {
		checkReadOnly();
		deleteDataByPath(getPath(extUri));
	}
}
