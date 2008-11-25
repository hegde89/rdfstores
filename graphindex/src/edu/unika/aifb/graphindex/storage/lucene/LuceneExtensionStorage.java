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
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.vp.LuceneStorage.Index;

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
		private Index index;
		private String so;
		
		public DocumentLoader(List<Integer> docIds, Index index, String val) {
			this.docIds = docIds;
			this.index = index;
			this.so = val;
		}
		
		public GTable<String> call() throws Exception {
			GTable<String> table = new GTable<String>("source", "target");
			try {
				for (int docId : docIds) {
					Document doc = getDocument(docId);
					
					if (index == Index.EPS) {
						String objects = doc.getField(index.getValField()).stringValue();
						StringSplitter splitter = new StringSplitter(objects, "\n");
						
						String s;
						while ((s = splitter.next()) != null)
							table.addRow(new String[] { so, s });
					}
					else {
						String subjects = doc.getField(index.getValField()).stringValue();
						StringSplitter splitter = new StringSplitter(subjects, "\n");
						
						String s;
						while ((s = splitter.next()) != null)
							table.addRow(new String[] { s, so });
					}
				}
			} catch (StorageException e) {
				e.printStackTrace();
			}
			
			return table;
		}
//		private List<Integer> docIds;
//		private String allowedSubject;
//		
//		public DocumentLoader(List<Integer> docIds, String allowedSubject) {
//			this.docIds = docIds;
//			this.allowedSubject = allowedSubject;
//		}
//		
//		public GTable<String> call() throws Exception {
//			GTable<String> table = new GTable<String>("source", "target");
//			try {
//				for (int docId : docIds) {
//					Document doc = getDocument(docId);
//					addToTable(table, doc, allowedSubject);
//				}
//			} catch (StorageException e) {
//				e.printStackTrace();
//			}
//			
//			return table;
//		}
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
				if (t.field().equals(Index.EPO.getIndexField())) {
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
	
	private Term getTerm(Index index, String val) {
		return new Term(index.getIndexField(), val);
	}
	
	private Term getTerm(Index index, String... val) {
		return getTerm(index, getPath(val));
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
	
	private Map<String,Boolean> htcache = new HashMap<String,Boolean>();
	
	public boolean hasTriples(Index index, String ext, String property, String so) throws StorageException  {
		try {
			String s = ext + "__" + property + "__" + so;
			Boolean value = htcache.get(s);
			if (value != null)
				return value.booleanValue();
			long start = System.currentTimeMillis();
			Query q = new TermQuery(getTerm(index, ext, property, so));
			Hits hits = m_searcher.search(q);
			boolean has = hits.length() > 0;
			log.debug("ht q: " + q + ": " + has + " {" + (System.currentTimeMillis() - start) + " ms}");
			htcache.put(s, has);
			return has;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	public void addTriples(Index index, String ext, String property, String so, List<String> values) throws StorageException {
		Collections.sort(values);
		
		StringBuilder sb = new StringBuilder();
		for (String s : values)
			sb.append(s).append('\n');
	
		Document doc = new Document();
		doc.add(new Field(index.getIndexField(), getPath(ext, property, so), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(index.getValField(), sb.toString(), Field.Store.YES, Field.Index.NO));
		
		try {
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public List<GTable<String>> getIndexTables(Index index, String ext, String property) throws StorageException {
		m_timings.start(Timings.DATA);
		long start = System.currentTimeMillis();
	
		PrefixQuery pq = new PrefixQuery(getTerm(index, ext, property));
		String query = pq.toString();
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		try {
			BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
			for (BooleanClause bc : bq.getClauses())
				queries.add((TermQuery)bc.getQuery());
		} catch (IOException e1) {
			throw new StorageException(e1);
		}
		
		List<Object[]> dis = new ArrayList<Object[]>();
		for (TermQuery q : queries) {
			String term = q.getTerm().text();
			String so = term.substring(term.indexOf("__", term.indexOf("__") + 2) + 2, term.lastIndexOf("__"));
			dis.add(new Object[] { so, getDocumentIds(q) });
		}
		
		List<GTable<String>> tables = new ArrayList<GTable<String>>();
		int x = 0;
		for (Object[] o : dis) {
			String so = (String)o[0];
			GTable<String> table = new GTable<String>("source", "target");
			for (int docId : (List<Integer>)o[1]) {
				Document doc = getDocument(docId);
				
				if (index == Index.EPS) {
					String objects = doc.getField(index.getValField()).stringValue();
					StringSplitter splitter = new StringSplitter(objects, "\n");
					
					String s;
					while ((s = splitter.next()) != null)
						table.addRow(new String[] { so, s });
				}
				else {
					String subjects = doc.getField(index.getValField()).stringValue();
					StringSplitter splitter = new StringSplitter(subjects, "\n");
					
					String s;
					while ((s = splitter.next()) != null)
						table.addRow(new String[] { s, so });
				}
			}
			x+= table.rowCount();
			tables.add(table);
		}
		log.debug("q: " + query + " (" + x + ") {" + (System.currentTimeMillis() - start) + " ms}");
		m_timings.end(Timings.DATA);
		return tables;
	}
	
	public GTable<String> getIndexTable(Index index, String ext, String property, String so) throws StorageException {
		m_timings.start(Timings.DATA);
		long start = System.currentTimeMillis();

		TermQuery tq = new TermQuery(getTerm(index, ext, property, so));
			
		GTable<String> table = new GTable<String>("source", "target");
		int docs = 0;
		
		long ds = System.currentTimeMillis();
		try {
			
			Future<List<Integer>> future1 = m_queryExecutor.submit(new QueryExecutor(tq));
			List<Integer> docIds = future1.get();
			ds = System.currentTimeMillis() - ds;
			
			long dr = 0;
			if (docIds.size() > 0) {
				docs += docIds.size();
				
				dr = System.currentTimeMillis();
				Future<GTable<String>> future2 = m_documentLoader.submit(new DocumentLoader(docIds, index, so));
				table = future2.get();
				dr = System.currentTimeMillis() - dr;
				
				if (index == Index.EPO)
					table.setSortedColumn(0);
				else
					table.setSortedColumn(1);
			}
			
			log.debug("q: " + tq + " (" + docs + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) + " ms, " + ds + ", " + dr + "}");
		} catch (InterruptedException e) {
			throw new StorageException(e);
		} catch (ExecutionException e) {
			throw new StorageException(e);
		}
		
		m_timings.end(Timings.DATA);
		return table;
	}
	
	public void mergeExtensions() throws StorageException, IOException {
//		TermEnum te = m_reader.terms();
//		while (te.next()) {
//			if (te.docFreq() > 1) {
//				Term t = te.term();
//				String[] path = getPathFromTerm(t);
//				List<Document> docs = getDocuments(new TermQuery(t));
//				Set<Subject> subjects = new HashSet<Subject>();
//				for (Document doc : docs) {
//					for (String s : doc.getField(FIELD_SUBJECT).stringValue().split("\n")) {
//						String[] sp = s.split("\t");
//						subjects.add(new Subject(sp[0], sp[1]));
//					}
//				}
//				SetExtensionSegment es = new SetExtensionSegment(path[0], path[1], path[2]);
//				es.setSubjects(subjects);
//				m_writer.updateDocument(t, segmentToDocument(es));
//			}
//		}
//		log.debug("optimizing...");
//		m_writer.optimize();
	}
	
//	private void deleteDataByPath(String queryPath) throws IOException {
//		List<Term> terms = new ArrayList<Term>();
//		
//		PrefixQuery q = new PrefixQuery(getTermForPath(queryPath));
//		BooleanQuery bq = (BooleanQuery)q.rewrite(m_reader);
//		
//		for (BooleanClause bc : bq.getClauses())
//			terms.add(((TermQuery)bc.getQuery()).getTerm());
//		log.debug("delete: " + queryPath);
//		m_writer.deleteDocuments(terms.toArray(new Term[]{}));
//		if (!bulkUpdating()) {
//			m_writer.flush();
//			reopen();
//		}
//	}
	
	public void deleteData(String extUri) throws IOException {
//		checkReadOnly();
//		deleteDataByPath(getPath(extUri));
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
		logger.debug("htcache: " + htcache.keySet().size());
		
		m_queriesFromCache.clear();
		m_queriesFromDisk.clear();
		m_docCacheHits = m_docCacheMisses = 0;
	}
}
