package edu.unika.aifb.graphindex.storage.lucene;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

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
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Timings;

public class LuceneExtensionStorage extends AbstractExtensionStorage {
	
	private String m_directory;
	public IndexWriter m_writer;
	public IndexReader m_reader;
	public IndexSearcher m_searcher;
	private LRUCache<Integer,Document> m_docCache;
	private LRUCache<String,GTable<String>> m_tableCache;
	private Timings m_timings;
	private Map<String,Integer> m_queriesFromDisk, m_queriesFromCache;
	private int m_docCacheHits;
	private int m_docCacheMisses;
	private int m_tableCacheHits, m_tableCacheMisses;
	
	private ExecutorService m_queryExecutor;
	private ExecutorService m_documentLoader;
	
	private final String FIELD_EXT = "ext";
	public final String FIELD_SUBJECT = "subject";
	private final String FIELD_PROPERTY = "property";
	private final String FIELD_OBJECT = "object";
	private final String EXT_PATH_SEP = "__";
	public final static Logger log = Logger.getLogger(LuceneExtensionStorage.class);
	
	private class QueryExecutor implements Callable<List<Integer>> {
		private Query q;
		
		public QueryExecutor(Query q) {
			this.q = q;
		}
		
		public List<Integer> call() {
			try {
				
				List<Integer> docIds = getDocumentIds(q);
				return docIds;
			} catch (StorageException e) {
				e.printStackTrace();
			}
			
			return new ArrayList<Integer>();
		}
	}
	
	private class DocumentLoader implements Callable<GTable<String>> {
		private List<Integer> docIds;
		private String allowedSubject;
		
		public DocumentLoader(List<Integer> docIds, String allowedSubject) {
			this.docIds = docIds;
			this.allowedSubject = allowedSubject;
		}
		
		public GTable<String> call() throws Exception {
			GTable<String> table = new GTable<String>("source", "target");
			try {
				for (int docId : docIds) {
					Document doc = getDocument(docId);
					addToTable(table, doc, allowedSubject);
				}
			} catch (StorageException e) {
				e.printStackTrace();
			}
			
			return table;
		}
		
	}
	
	public LuceneExtensionStorage(String directory) {
		m_directory = directory;
		m_timings = new Timings();
		
		m_queriesFromCache = new HashMap<String,Integer>();
		m_queriesFromDisk = new HashMap<String,Integer>();
		
		m_queryExecutor = Executors.newFixedThreadPool(20);
		m_documentLoader = Executors.newFixedThreadPool(10);
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
			
			log.info("table cache size: " + m_manager.getIndex().getTableCacheSize());
			m_tableCache = new LRUCache<String,GTable<String>>(m_manager.getIndex().getTableCacheSize());
			
			log.info("doc cache size: " + m_manager.getIndex().getDocumentCacheSize());
			m_docCache = new LRUCache<Integer,Document>(m_manager.getIndex().getDocumentCacheSize());
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		m_manager.getIndex().getCollector().addTimings(m_timings);
		BooleanQuery.setMaxClauseCount(1048576);
		log.debug("gzip: " + m_manager.getIndex().isGZip());
	}
	
	public void close() {
		try {
			if (!m_readonly && m_writer != null)
				m_writer.close();
			if (m_searcher != null)
				m_searcher.close();
			m_queryExecutor.shutdown();
			m_documentLoader.shutdown();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void updateCacheSizes() {
		if (m_manager.getIndex().getTableCacheSize() != m_tableCache.cacheSize()) {
			log.info("table cache size: " + m_manager.getIndex().getTableCacheSize());
			m_tableCache = new LRUCache<String,GTable<String>>(m_manager.getIndex().getTableCacheSize());
		}
		
		if (m_manager.getIndex().getDocumentCacheSize() != m_tableCache.cacheSize()) {
			log.info("doc cache size: " + m_manager.getIndex().getDocumentCacheSize());
			m_docCache = new LRUCache<Integer,Document>(m_manager.getIndex().getDocumentCacheSize());
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
	
	public void clearCaches() throws StorageException {
		m_tableCache = new LRUCache<String,GTable<String>>(m_manager.getIndex().getTableCacheSize());
		m_docCache = new LRUCache<Integer,Document>(m_manager.getIndex().getDocumentCacheSize());
		try {
			m_reader.flush();
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
	
	private Term getTermForPath(String path) {
		return new Term(FIELD_EXT, path);
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
			m_docCacheHits++;
			Document doc = m_docCache.get(docId);
			if (doc == null) {
				m_docCacheMisses++;
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
	
	private interface DocumentIdCollector {
		public void collect(int docId);
	}
	
	private void search(Query q, final DocumentIdCollector collector) throws StorageException {
		try {
			m_searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					collector.collect(docId);
				}
			});
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	private List<Document> getDocuments(Query q) throws StorageException {
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
	
	private byte[] deflate(String input) {
		return null;
	}
	
	private String inflate(byte[] input) {
		try {
			InputStreamReader isr = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(input)));
			StringBuffer output = new StringBuffer();
			char[] buf = new char [2048];
			
			int x;
			while ((x = isr.read(buf)) != -1)
				output.append(buf);
			
			return output.toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	 
	private void addToTable(GTable<String> table, Document doc, String allowedSubject) {
		String subjectString;
		if (m_manager.getIndex().isGZip())
			subjectString = inflate(doc.getField(FIELD_SUBJECT).binaryValue());
		else
			subjectString = doc.getField(FIELD_SUBJECT).stringValue();
		
		StringSplitter splitter = new StringSplitter(subjectString, "\n");
		
		String object = doc.getField(FIELD_OBJECT).stringValue();

//		for (String s : subjectStrings) {
//			String[] t = s.split("\t");
//			table.addRow(new String [] { t[0], object });
//		}
//		log.debug(object);
//		for (String s : subjectStrings) {
		m_timings.start(Timings.SUBJECT_FILTER);
		String s;
		while ((s = splitter.next()) != null) {
//			log.debug(s);
//			if (allowedSubject != null)
//				log.debug(s);
			if (allowedSubject == null || s.equals(allowedSubject))
				table.addRow(new String [] { s, object });
		}
		m_timings.end(Timings.SUBJECT_FILTER);
	}
	
	private class Timer {
		public long val;
		private long cur;
		
		public Timer() {
			val = 0;
		}
		
		public void start() {
			cur = System.currentTimeMillis();
		}
		
		public void end() {
			val += System.currentTimeMillis() - cur;
			cur = 0;
		}
	}
	
	public GTable<String> getTable(String extUri, String property, String object, final String allowedSubject) throws StorageException {
		m_timings.start(Timings.DATA);
		long start = System.currentTimeMillis();
		
		String q = getTerm(extUri, property, object).toString();

		m_tableCacheHits++;
		
		GTable<String> table = m_tableCache.get(object == null ? getPath(extUri, property) : getPath(extUri, property, object));
		
		if (table != null) {
			synchronized (m_queriesFromCache) {
				if (m_queriesFromCache.containsKey(q))
					m_queriesFromCache.put(q, m_queriesFromCache.get(q) + 1);
				else
					m_queriesFromCache.put(q, 1);
			}
			log.debug("q: " + getTerm(extUri, property, object) + " (from cache) {" + (System.currentTimeMillis() - start) + " ms}");
			return table;
		}
		
		m_tableCacheMisses++;

		synchronized (m_queriesFromDisk) {
			if (m_queriesFromDisk.containsKey(q))
				m_queriesFromDisk.put(q, m_queriesFromDisk.get(q) + 1);
			else
				m_queriesFromDisk.put(q, 1);
		}
		
		GTable<String> resultTable = new GTable<String>("source", "target");
		
		Query tq;
		if (object != null)
			tq = new TermQuery(getTerm(extUri, property, object));
		else
			tq = new PrefixQuery(getTerm(extUri, property));
		log.debug("q: " + tq + " (as: " + allowedSubject + ")");

		final Timer retrieval = new Timer();
		final Timer building = new Timer();
		int docs = 0;
		
//		search(tq, new DocumentIdCollector() {
//			public void collect(int docId) {
//				try {
//					retrieval.start();
//					Document doc = getDocument(docId);
//					retrieval.end();
//
//					building.start();
//					addToTable(resultTable, doc, allowedSubject);
//					building.end();
//				} catch (StorageException e) {
//					e.printStackTrace();
//				}
//			}
//		});
		
//		List<Integer> docIds = getDocumentIds(tq);
//		for (int docId : docIds) {
//			retrieval.start();
//			Document doc = getDocument(docId);
//			retrieval.end();
//			
//			building.start();
//			addToTable(resultTable, doc, allowedSubject);
//			building.end();
//			
//			docs++;
//		}
		
		try {
			Future<List<Integer>> f = m_queryExecutor.submit(new QueryExecutor(tq));
			List<Integer> docIds = f.get();
			
			Future<GTable<String>> f2 = m_documentLoader.submit(new DocumentLoader(docIds, allowedSubject));
			resultTable = f2.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		if (allowedSubject == null)
			m_tableCache.put(object == null ? getPath(extUri, property) : getPath(extUri, property, object), resultTable);

		log.debug("  " + docs + " docs, " + resultTable.rowCount() + " triples {" + (System.currentTimeMillis() - start) + " ms, dr: " + retrieval.val + ", db: " + building.val + "}");
		m_timings.end(Timings.DATA);
		
		return resultTable;
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
	
	public void logStats(Logger logger) {
//		logger.debug("queries from cache:");
		int cache = 0;
		for (String q : m_queriesFromCache.keySet())
			if (m_queriesFromCache.get(q) > 1)
				cache += m_queriesFromCache.get(q) - 1;
//				logger.debug(" " + q + " " + m_queriesFromCache.get(q));
//		logger.debug("queries from disk:");
		int disk = 0;
		for (String q : m_queriesFromDisk.keySet())
			if (m_queriesFromDisk.get(q) > 1)
				disk += m_queriesFromDisk.get(q) - 1;
//				logger.debug(" " + q + " " + m_queriesFromDisk.get(q));
		logger.debug("doccache: " + m_docCacheMisses + "/" + m_docCacheHits);
		logger.debug("tablecache: " + m_tableCacheMisses + "/" + m_tableCacheHits);
		logger.debug("dup cache: " + cache);
		logger.debug("dup disk: " + disk);
		
		m_queriesFromCache.clear();
		m_queriesFromDisk.clear();
		m_docCacheHits = m_docCacheMisses = 0;
	}
}
