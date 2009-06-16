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
import java.util.TreeSet;
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
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
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
	private LRUCache<String,Set<String>> m_dataSetCache;
	private LRUCache<String,List<String>> m_dataListCache;
	private Timings m_timings;
//	private Map<String,Integer> m_queriesFromDisk, m_queriesFromCache;
	private int m_docCacheHits;
	private int m_docCacheMisses;
	
	private final String EXT_PATH_SEP = "__";
	public final static Logger log = Logger.getLogger(LuceneExtensionStorage.class);
	
	public LuceneExtensionStorage(String directory) {
		m_directory = directory;
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
			
			if (m_manager != null && m_manager.getIndex() != null) {
				log.info("doc cache size: " + m_manager.getIndex().getDocumentCacheSize());
				m_docCache = new LRUCache<Integer,Document>(m_manager.getIndex().getDocumentCacheSize());
			}
			else
				m_docCache = new LRUCache<Integer,Document>(5000);
			
			m_dataSetCache = new LRUCache<String,Set<String>>(50000);
			m_dataListCache = new LRUCache<String,List<String>>(50000);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (m_manager != null && m_manager.getIndex() != null) {
			m_manager.getIndex().getCollector().addTimings(m_timings);
		}
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
	
	public void updateCacheSizes() {
		if (m_manager.getIndex().getDocumentCacheSize() != m_docCache.cacheSize()) {
			m_docCache = new LRUCache<Integer,Document>(m_manager.getIndex().getDocumentCacheSize());
		}
	}
	
	public void optimize() throws StorageException {
		if (!m_readonly) {
			try {
				m_searcher.close();
				m_reader.close();
				m_writer.optimize();
				m_reader = IndexReader.open(m_directory);
				m_searcher = new IndexSearcher(m_reader);
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
//		m_queriesFromCache.clear();
//		m_queriesFromDisk.clear();
		htcache.clear();
		m_subjectExtCache.clear();
		m_objectExtCache.clear();
		m_extCacheHits = 0;
		m_timings.reset();
		m_o2e.clear();
		m_docCacheHits = m_docCacheMisses = 0;
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
	
	public String concat(String[] values, int length) {
		String path = "";
		for (int i = 0; i < length; i++)
			path += values[i] + EXT_PATH_SEP;
		return path;
	}
	
	private Term getTerm(IndexDescription index, String val) {
		return new Term(index.getIndexFieldName(), val);
	}
	
	private Term getTerm(IndexDescription index, String... val) {
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
//		log.debug(q + "  " + docIds.size() + " docs");
		
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

	public boolean hasTriples(IndexDescription index, String ext, String property, String so) throws StorageException  {
		try {
			m_timings.start(Timings.LOAD_HT);
			String s = new StringBuilder().append(ext).append("__").append(property).append("__").append(so).toString();
			Boolean value = htcache.get(s);
			if (value != null) {
				m_timings.end(Timings.LOAD_HT);
				return value.booleanValue();
			}
			long start = System.currentTimeMillis();
			Query q = so != null ? new TermQuery(getTerm(index, property, so, ext)) : new PrefixQuery(getTerm(index, ext, property));
			Hits hits = m_searcher.search(q);
			boolean has = hits.length() > 0;
//			log.debug("ht q: " + q + ": " + has + " {" + (System.currentTimeMillis() - start) + " ms}");
			htcache.put(s, has);
			m_timings.end(Timings.LOAD_HT);
			return has;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public List<String> getData(IndexDescription index, String... indexFields) throws StorageException {
		m_timings.start(Timings.LOAD_DATA_LIST);
		
		TermQuery tq = new TermQuery(new Term(index.getIndexFieldName(), concat(indexFields, indexFields.length)));

		List<String> values = new ArrayList<String>(200);
		List<Integer> docIds = getDocumentIds(tq);
		if (docIds.size() > 0) {
			values.addAll(loadDocuments(docIds, index));
		}
		
		m_timings.end(Timings.LOAD_DATA_LIST);
		return values;
	}
	
	public Set<String> getDataSet(IndexDescription index, String... indexFields) throws StorageException {
		m_timings.start(Timings.LOAD_DATA_SET);

		String query = concat(indexFields, indexFields.length);
		Set<String> values = m_dataSetCache.get(query);
		if (values != null) {
			m_timings.end(Timings.LOAD_DATA_SET);
			return values;
		}
		
		TermQuery tq = new TermQuery(new Term(index.getIndexFieldName(), query));

		values = new HashSet<String>(200);
		List<Integer> docIds = getDocumentIds(tq);
		if (docIds.size() > 0) {
			values.addAll(loadDocuments(docIds, index));
		}
		
		m_dataSetCache.put(query, values);
		
		m_timings.end(Timings.LOAD_DATA_SET);
		return values;
	}

	public void addData(IndexDescription index, String indexKey, List<String> values, boolean sort) throws StorageException {
		if (sort)
			Collections.sort(values);
		addData(index, indexKey, values);
	}

	public void addData(IndexDescription index, String indexKey, Collection<String> values) throws StorageException {
		StringBuilder sb = new StringBuilder();
		for (String s : values)
			sb.append(s).append('\n');
		
		Document doc = new Document();
		doc.add(new Field(index.getIndexFieldName(), indexKey, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(index.getValueFieldName(), sb.toString(), Field.Store.YES, Field.Index.NO));
		
		try {
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void addTriples(IndexDescription index, String ext, String property, String so, List<String> values) throws StorageException {
		Collections.sort(values);
		
		StringBuilder sb = new StringBuilder();
		for (String s : values)
			sb.append(s).append('\n');
	
		Document doc = new Document();
		doc.add(new Field(index.getIndexFieldName(), getPath(property, so, ext), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(index.getValueFieldName(), sb.toString(), Field.Store.YES, Field.Index.NO));
		
		try {
			m_writer.addDocument(doc);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void createSEOE(Map<String,Set<String>> se, Set<String> oe) throws StorageException {
		try {
			for (String o : oe) {
				Document doc = new Document();
				doc.add(new Field("oe", o, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				
				m_writer.addDocument(doc);
			}
			
			for (String s : se.keySet()) {
				Set<String> exts = se.get(s);
				StringBuilder sb = new StringBuilder();
				for (String ext : exts)
					sb.append(ext).append("\n");
				
				Document doc = new Document();
				doc.add(new Field("se", s, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field("e", sb.toString(), Field.Store.YES, Field.Index.NO));
				
				m_writer.addDocument(doc);
			}
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	private List<String> loadDocuments(List<Integer> docIds, IndexDescription index) throws StorageException {
		List<String> values = new ArrayList<String>(100);
		for (int docId : docIds) {
			Document doc = getDocument(docId);

			StringSplitter splitter = new StringSplitter(doc.getField(index.getValueFieldName()).stringValue(), "\n");
			String s;
			while ((s = splitter.next()) != null)
				values.add(s);
		}
		return values;
	}
	
	private void loadDocuments(GTable<String> table, List<Integer> docIds, IndexDescription index, String so) throws StorageException {
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			if (index.getValueField() == DataField.OBJECT) {
				String objects = doc.getField(index.getValueFieldName()).stringValue();
				StringSplitter splitter = new StringSplitter(objects, "\n");
				
				String s;
				while ((s = splitter.next()) != null)
					table.addRow(new String[] { so, s });
			}
			else {
				String subjects = doc.getField(index.getValueFieldName()).stringValue();
				StringSplitter splitter = new StringSplitter(subjects, "\n");
				
				String s;
				while ((s = splitter.next()) != null)
					table.addRow(new String[] { s, so });
			}
		}
	}
	
	public GTable<String> getIndexTable(String col1, String col2, IndexDescription index, String... indexFields) throws StorageException {
		m_timings.start(Timings.LOAD_ITS);
		GTable<String> table = new GTable<String>(col1, col2);

		String so = null;
		for (int i = 0; i < indexFields.length; i++)
			if ((index.getValueField() == DataField.OBJECT && index.getIndexFields().get(i) == DataField.SUBJECT) ||
				(index.getValueField() == DataField.SUBJECT && index.getIndexFields().get(i) == DataField.OBJECT))
				so = indexFields[i];
			

		Query q;
		if (indexFields.length < index.getIndexFields().size()) 
			q = new PrefixQuery(getTerm(index, indexFields));
		else
			q = new TermQuery(getTerm(index, indexFields));
		
		List<Integer> docIds = getDocumentIds(q);
		loadDocuments(table, docIds, index, so);
		
		m_timings.end(Timings.LOAD_ITS);
		return table;
	}
	
	public List<GTable<String>> getIndexTables(IndexDescription index, String ext, String property) throws StorageException {
//		m_timings.start(Timings.DATA);
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
				
				if (index.getValueField() == DataField.OBJECT) {
					String objects = doc.getField(index.getValueFieldName()).stringValue();
					StringSplitter splitter = new StringSplitter(objects, "\n");
					
					String s;
					while ((s = splitter.next()) != null)
						table.addRow(new String[] { so, s });
				}
				else {
					String subjects = doc.getField(index.getValueFieldName()).stringValue();
					StringSplitter splitter = new StringSplitter(subjects, "\n");
					
					String s;
					while ((s = splitter.next()) != null)
						table.addRow(new String[] { s, so });
				}
			}
			x+= table.rowCount();
			tables.add(table);
		}
//		log.debug("q: " + query + " (" + x + ") {" + (System.currentTimeMillis() - start) + " ms}");
//		m_timings.end(Timings.DATA);
		return tables;
	}
	
	public GTable<String> getIndexTable(IndexDescription index, String ext, String property, String so) throws StorageException {
		m_timings.start(Timings.LOAD_IT);
		long start = System.currentTimeMillis();

		TermQuery tq = new TermQuery(getTerm(index, property, so, ext));
			
		GTable<String> table = new GTable<String>("source", "target");
		int docs = 0;
		
		long ds = System.currentTimeMillis();
		
		List<Integer> docIds = getDocumentIds(tq);
		ds = System.currentTimeMillis() - ds;
		
		long dr = 0;
		if (docIds.size() > 0) {
			docs += docIds.size();
			
			loadDocuments(table, docIds, index, so);
			
			if (index.getValueField() == DataField.SUBJECT)
				table.setSortedColumn(0);
			else
				table.setSortedColumn(1);
		}
			
//		log.debug("q: " + tq + " (" + docs + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) + " ms, " + ds + ", " + dr + "}");
		
		m_timings.end(Timings.LOAD_IT);
		return table;
	}
	
	private LRUCache<String,Set<String>> m_subjectExtCache = new LRUCache<String,Set<String>>(400000);
	private LRUCache<String,Set<String>> m_objectExtCache = new LRUCache<String,Set<String>>(400000);
	private long m_extCacheHits = 0;
	
	private Map<String,String> m_o2e = new HashMap<String,String>(100000);
	
	public String getExtension(String object) throws StorageException {
		m_timings.start(Timings.LOAD_EXT_OBJECT);
		String ext = m_o2e.get(object);
		if (ext != null) {
			m_timings.end(Timings.LOAD_EXT_OBJECT);
			return ext;
		}
		
		PrefixQuery pq = new PrefixQuery(new Term(Index.OE.getIndexField(), object + "__"));
		
		BooleanQuery bq = null;
		try {
			bq = (BooleanQuery)pq.rewrite(m_reader);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		if (bq.getClauses().length == 0) {
			m_o2e.put(object, "");
			m_timings.end(Timings.LOAD_EXT_OBJECT);
			return "";
		}
		
		TermQuery tq = (TermQuery)(bq.getClauses()[0].getQuery());
		String term = tq.getTerm().text();
		ext = term.substring(term.indexOf("__") + 2);
		
		m_o2e.put(object, ext);
		m_timings.end(Timings.LOAD_EXT_OBJECT);
		
//		log.debug("eq: " + tq);
		
		return ext;
	}
	
	public boolean isValidObjectExtension(String object, String ext) throws StorageException {
		m_timings.start(Timings.LOAD_EXT_OBJECT);
		
		String cachedExt = m_o2e.get(object);
		if (cachedExt != null) {
			m_timings.end(Timings.LOAD_EXT_OBJECT);
			return cachedExt.equals(ext);
		}
		
		String term = new StringBuilder().append(object).append("__").append(ext).toString();
		try {
			TermDocs td = m_reader.termDocs(new Term(Index.OE.getIndexField(), term));
			boolean tdd = td.next();
			if (tdd) {
				m_o2e.put(object, ext);
				m_timings.end(Timings.LOAD_EXT_OBJECT);
				return true;
			}
			m_timings.end(Timings.LOAD_EXT_OBJECT);
			return false;
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public static int extLoaded = 0;
	public Set<String> getExtensions(IndexDescription index, String so) throws StorageException {
		m_timings.start(Timings.LOAD_EXT_SUBJECT);
		
		Set<String> exts;
		
		TermQuery tq = new TermQuery(new Term(index.getIndexFieldName(), so));

		exts = new HashSet<String>();	
		List<Integer> docIds = getDocumentIds(tq);
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			String sos = doc.getField(index.getValueFieldName()).stringValue();
			StringSplitter splitter = new StringSplitter(sos, "\n");
			String s;
			while ((s = splitter.next()) != null) {
				exts.add(s);
			}
		}
		
		m_timings.end(Timings.LOAD_EXT_SUBJECT);
		
		return exts;
	}
	
	public void mergeIndexDocuments(IndexDescription index) throws StorageException {
		try {
			TermEnum te = m_reader.terms(new Term(index.getIndexFieldName(), ""));
			do {
				Term t = te.term();
				
				if (!t.field().equals(index.getIndexFieldName()))
					break;
				
				List<Integer> docIds = getDocumentIds(new TermQuery(t));
				if (docIds.size() == 1)
					continue;
				
				TreeSet<String> values = new TreeSet<String>();
				for (int docId : docIds) {
					Document doc = getDocument(docId);
					values.add(doc.getField(index.getValueFieldName()).stringValue().trim());
				}
				m_writer.deleteDocuments(t);
				
				StringBuilder sb = new StringBuilder();
				for (String s : values)
					sb.append(s).append('\n');
				
				Document doc = new Document();
				doc.add(new Field(index.getIndexFieldName(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(index.getValueFieldName(), sb.toString(), Field.Store.YES, Field.Index.NO));
				m_writer.addDocument(doc);
			}
			while (te.next());
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
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
	
	public void reopenAndWarmUp() throws StorageException {
		try {
			m_searcher.close();
			m_reader.close();
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		try {
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		warmup();
	}
	
	public void warmUp(Set<String> queries) throws StorageException {
		for (String query : queries) {
			String[] t = query.split(" ", 2);
			Query q = new PrefixQuery(new Term(t[0], t[1]));
			List<Integer> docIds = getDocumentIds(q);
//			log.debug("warmup: " + q + " => " + docIds.size() + " doc ids");
		}
	}
	
	public void warmup() throws StorageException {
		long start = System.currentTimeMillis();

		// LUBM warmup
//		getDocumentIds(new TermQuery(new Term(Index.EPO.getIndexField(), "b1186__http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name__University1__")));
//		getDocumentIds(new TermQuery(new Term(Index.EPS.getIndexField(), "b1904__http://www.w3.org/1999/02/22-rdf-syntax-ns#type__http://www.University1.edu__")));
//		getDocumentIds(new TermQuery(new Term(Index.OE.getIndexField(), "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University__b1904")));
//		getDocumentIds(new TermQuery(new Term(Index.SE.getIndexField(), "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University")));

		// DBLP warmup
//		getDocumentIds(new TermQuery(new Term(Index.EPO.getIndexField(), "b257774__http://lsdis.cs.uga.edu/projects/semdis/opus#book_title__WWW__")));
//		getDocumentIds(new TermQuery(new Term(Index.EPS.getIndexField(), "b261911__http://lsdis.cs.uga.edu/projects/semdis/opus#editor__http://dblp.uni-trier.de/rec/bibtex/conf/www/2006__")));
//		getDocumentIds(new TermQuery(new Term(Index.OE.getIndexField(), "http://dblp.uni-trier.de/rec/bibtex/conf/www/2004__b48179")));
//		getDocumentIds(new TermQuery(new Term(Index.SE.getIndexField(), "http://dblp.uni-trier.de/rec/bibtex/conf/www/2005")));
		
		log.debug("warmup in " + (System.currentTimeMillis() - start) + " ms");
	}
	
	public void logStats(Logger logger) {
//		int cache = 0;
//		for (String q : m_queriesFromCache.keySet())
//			if (m_queriesFromCache.get(q) > 1)
//				cache += m_queriesFromCache.get(q) - 1;
		
//		int disk = 0;
//		for (String q : m_queriesFromDisk.keySet())
//			if (m_queriesFromDisk.get(q) > 1)
//				disk += m_queriesFromDisk.get(q) - 1;
		
		logger.debug("doccache: " + m_docCacheMisses + "/" + m_docCacheHits);
//		logger.debug("dup cache: " + cache);
//		logger.debug("dup disk: " + disk);
//		logger.debug("htcache: " + htcache.keySet().size());
//		logger.debug("subcache: " + m_subjectExtCache.usedEntries());
//		logger.debug("objcache: " + m_objectExtCache.usedEntries());
//		logger.debug("extcachehits: " + m_extCacheHits);
//		logger.debug("o2e: " + m_o2e.keySet().size());
	}

}
