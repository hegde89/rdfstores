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
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.ListExtensionSegment;
import edu.unika.aifb.graphindex.data.SetExtensionSegment;
import edu.unika.aifb.graphindex.data.Subject;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.query.Table;
import edu.unika.aifb.graphindex.storage.AbstractExtensionStorage;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public class LuceneExtensionStorage extends AbstractExtensionStorage {
	
	private String m_directory;
	public IndexWriter m_writer;
	public IndexReader m_reader;
	public IndexSearcher m_searcher;
	private LRUCache<Integer,Document> m_docCache;
	private LRUCache<String,List<Triple>> m_tripleCache;
	private LRUCache<String,GTable<String>> m_tableCache;
	private Timings m_timings;
	
	private final String FIELD_EXT = "ext";
	public final String FIELD_SUBJECT = "subject";
	private final String FIELD_PROPERTY = "property";
	private final String FIELD_OBJECT = "object";
	private final String FIELD_TYPE = "type";
	private final String FIELD_VAL = "value";
	private final String TYPE_EXTLIST = "extension_list";
	private final String EXT_PATH_SEP = "__";
	public final static Logger log = Logger.getLogger(LuceneExtensionStorage.class);
	
	public LuceneExtensionStorage(String directory) {
		m_directory = directory;
		m_docCache = new LRUCache<Integer,Document>(1);
		m_tripleCache = new LRUCache<String,List<Triple>>(5);
		m_tableCache = new LRUCache<String,GTable<String>>(1);
		m_timings = new Timings();
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
		
		m_manager.getIndex().getCollector().addTimings(m_timings);
		BooleanQuery.setMaxClauseCount(1048576);
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
	
	public void checkReadOnly() {
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
	
	public void reopen() throws CorruptIndexException, IOException {
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
	
	public Document getDocument(int docId) throws StorageException {
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
	
	public List<Document> getDocuments(Query q) throws StorageException {
		long start = System.currentTimeMillis();
		
		try {
			final List<Integer> docIds = new ArrayList<Integer>();
			m_searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					docIds.add(docId);
				}
			});
			log.debug("  " + docIds.size() + " docs");
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

//			log.debug("query: " + q + " (" + docIds.size() + " docs) {" + (System.currentTimeMillis() - start) + " ms, s: " + search + " ms, r: " + retrieval + " ms}");
			
			return docs;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public boolean hasTriples(String ext, String propertyUri, String object) throws StorageException  {
		try {
			long start = System.currentTimeMillis();
			Query q = new TermQuery(getTerm(ext, propertyUri, object));
			Hits hits = m_searcher.search(q);
//			log.debug("hasDocs q: " + q + " (" + docIds.size() + ") {" + (System.currentTimeMillis() - start) + " ms}");
			return hits.length() > 0;
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
	
	public Document segmentToDocument(ExtensionSegment es) {
		Document doc = new Document();
		doc.add(new Field(FIELD_EXT, getPath(es), Field.Store.YES, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(FIELD_OBJECT, es.getObject(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(FIELD_SUBJECT, es.toSubjectString(), Field.Store.YES, Field.Index.NO));
		return doc;
	}
	
	public ExtensionSegment documentToSegment(Document doc, String extUri, String propertyUri) {
		ListExtensionSegment es = new ListExtensionSegment(extUri, propertyUri, doc.getField(FIELD_OBJECT).stringValue());
		String[] subjectStrings = doc.getField(FIELD_SUBJECT).stringValue().split("\n");
		List<Subject> list = new ArrayList<Subject>();
		for (String s : subjectStrings) {
			String[] t = s.split("\t");
			list.add(new Subject(t[0], t[1]));
		}
		es.setSubjects(list);
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
	
	public List<ExtensionSegment> loadExtensionSegments(String extUri, String property) throws StorageException {
		long start = System.currentTimeMillis();
		
		PrefixQuery pq = new PrefixQuery(getTerm(extUri, property));
		List<Document> docs = getDocuments(pq);
		
		long dr = System.currentTimeMillis() - start;
		
		List<ExtensionSegment> segments = new ArrayList<ExtensionSegment>(docs.size());
		int triples = 0;
		for (Document doc : docs) {
			ExtensionSegment es = documentToSegment(doc, extUri, property);
			triples += es.size();
			segments.add(es);
		}
		
		long build = System.currentTimeMillis() - start - dr;
		
		if (docs.size() > 0)
			log.debug("meq: " + pq + " (" + docs.size() + " docs, " + triples + " triples) {" + (System.currentTimeMillis() - start) + " ms, dr: " + dr + ", b: " + build + "}");
		
		return segments;
	}
	
	public ExtensionSegment loadExtensionSegment(String extUri, String property, String object) throws StorageException {
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
			log.debug("seq: " + tq + " (" + docs.size() + " docs" + (es != null ? ", " + es.getSubjects().size() + " triples" : "") + ") {" + (System.currentTimeMillis() - start) + " ms, dr: " + dr + ", b: " + build + "}");
		
		return es;
	}
	
	private void addToTable(GTable<String> table, Document doc, String allowedSubject) {
		String[] subjectStrings = doc.getField(FIELD_SUBJECT).stringValue().split("\n");
		String object = doc.getField(FIELD_OBJECT).stringValue();

//		for (String s : subjectStrings) {
//			String[] t = s.split("\t");
//			table.addRow(new String [] { t[0], object });
//		}
//		log.debug(object);
		for (String s : subjectStrings) {
//			log.debug(s);
//			if (allowedSubject != null)
//				log.debug(s);
			if (allowedSubject == null || s.equals(allowedSubject))
				table.addRow(new String [] { s, object });
		}
	}
	
	public GTable<String> getTable(String extUri, String property, String object, String allowedSubject) throws StorageException {
		m_timings.start(Timings.DATA);
		long start = System.currentTimeMillis();

		GTable<String> table = m_tableCache.get(object == null ? getPath(extUri, property) : getPath(extUri, property, object));
		
		if (table != null) {
			log.debug("q: " + getTerm(extUri, property, object) + " (from cache) {" + (System.currentTimeMillis() - start) + " ms}");
			return table;
		}

		table = new GTable<String>("source", "target");
		
		Query tq;
		if (object != null)
			tq = new TermQuery(getTerm(extUri, property, object));
		else
			tq = new PrefixQuery(getTerm(extUri, property));
		log.debug("q: " + tq + " (as: " + allowedSubject + ")");
		
		List<Integer> docIds = getDocumentIds(tq);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			addToTable(table, doc, allowedSubject);
		}
		
		m_tableCache.put(object == null ? getPath(extUri, property) : getPath(extUri, property, object), table);

		log.debug("  " + docIds.size() + " docs, " + table.rowCount() + " triples {" + (System.currentTimeMillis() - start) + " ms}");
		m_timings.end(Timings.DATA);
		
		return table;
	}
	
	public List<Triple> getTriples(String extUri, String property, String object) throws StorageException {
		List<Triple> triples = m_tripleCache.get(getPath(extUri, property, object));
		if (triples == null) {
			ExtensionSegment es = loadExtensionSegment(extUri, property, object);
			if (es != null)
				triples = es.toTriples();
			else
				triples = new ArrayList<Triple>();
			m_tripleCache.put(getPath(extUri, property, object), triples);
		}
		return triples;
	}
	
	public List<Triple> getTriples(String extUri, String property) throws StorageException {
		List<Triple> triples = m_tripleCache.get(getPath(extUri, property));
		if (triples == null) {
			triples = new ArrayList<Triple>();
			List<ExtensionSegment> ess = loadExtensionSegments(extUri, property);

			long start = System.currentTimeMillis();
			
			if (ess.size() == 1)
				triples = ess.get(0).toTriples();
			else 
				for (ExtensionSegment es : ess)
					triples.addAll(es.toTriples());
			
//			log.debug(System.currentTimeMillis() - start);
			m_tripleCache.put(getPath(extUri, property), triples);
		}
		return triples;
	}
	
	public void mergeExtensions() throws StorageException, IOException {
		TermEnum te = m_reader.terms();
		while (te.next()) {
			if (te.docFreq() > 1) {
				Term t = te.term();
//				log.debug("term " + t + " docfreq > 1");
				String[] path = getPathFromTerm(t);
				List<Document> docs = getDocuments(new TermQuery(t));
				Set<Subject> subjects = new HashSet<Subject>();
				for (Document doc : docs) {
					for (String s : doc.getField(FIELD_SUBJECT).stringValue().split("\n")) {
						String[] sp = s.split("\t");
						subjects.add(new Subject(sp[0], sp[1]));
					}
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
