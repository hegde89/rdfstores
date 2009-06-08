package edu.unika.aifb.graphindex.vp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.storage.lucene.LRUCache;
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Timings;

public class LuceneStorage {
	private String m_directory;
	public IndexWriter m_writer;
	public IndexReader m_reader;
	public IndexSearcher m_searcher;
	private ExecutorService m_queryExecutor;
	private ExecutorService m_documentLoader;
	private boolean m_readonly;
	private boolean m_merged;
	private Map<String,Integer> m_objectCardinalities;

	public static final class Index {
		public static final Index PO = new Index("po", "s");
		public static final Index PS = new Index("ps", "o");
		public static final Index OP = new Index("op", "s");
		public static final Index SP = new Index("sp", "o");
		
		private String m_indexField;
		private String m_valField;
		
		private Index(String indexField, String valField) { 
			m_indexField = indexField;
			m_valField = valField;
		}
		
		public String getIndexField() {
			return m_indexField;
		}
		
		public String getValField() {
			return m_valField;
		}
	}
	
	private static final String FIELD_SUBJECT = "s";
	private static final String FIELD_PROPERTY = "p";
	private static final String FIELD_OBJECT = "o";
	
	private static final Logger log = Logger.getLogger(LuceneStorage.class);

	@SuppressWarnings("unchecked")
	public LuceneStorage(String directory) {
		m_directory = directory;
		
		m_queryExecutor = Executors.newFixedThreadPool(1);
		m_documentLoader = Executors.newFixedThreadPool(1);
		
		m_objectCardinalities = new HashMap<String,Integer>();
		try {
			Map cmap = (Map)Yaml.load(new File(directory + "/object_cardinalities"));
			for (String prop : (Set<String>)cmap.keySet()) {
				m_objectCardinalities.put(prop, (Integer)cmap.get(prop));
			}
			log.debug("object cards. loaded: " + m_objectCardinalities);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		try {
			m_readonly = readonly;
			if (!readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(512);
				m_writer.setMergeFactor(20);
			}
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (new File(m_directory + "/merged").exists())
			m_merged = true;
		log.debug("merged index: " + m_merged);
		
		BooleanQuery.setMaxClauseCount(1048576);
	}
	
	public void warmup() throws StorageException {
		long start = System.currentTimeMillis();

		// LUBM warmup
		getDocumentIds(new TermQuery(new Term(Index.PO.getIndexField(), "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse__http://www.Department6.University29.edu/Course10")));
		getDocumentIds(new TermQuery(new Term(Index.PS.getIndexField(), "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf__http://www.Department3.University0.edu/GraduateStudent38")));

		// DBLP warmup
//		getDocumentIds(new TermQuery(new Term(Index.PO.getIndexField(), "http://lsdis.cs.uga.edu/projects/semdis/opus#isIncludedIn__http://dblp.uni-trier.de/rec/bibtex/conf/www/2004")));
//		getDocumentIds(new TermQuery(new Term(Index.PS.getIndexField(), "http://lsdis.cs.uga.edu/projects/semdis/opus#editor__http://dblp.uni-trier.de/rec/bibtex/conf/www/2004")));
		
		log.debug("warmup in " + (System.currentTimeMillis() - start) + " ms");
	}
	
	public void reopenAndWarmup() throws StorageException {
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
	private LRUCache<Integer,Document> m_docCache = new LRUCache<Integer,Document>(10000);
	
	public Integer getObjectCardinality(String property) {
		return m_objectCardinalities.get(property);
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

	public void addTriple(String subject, String property, String object) {
		if (m_readonly)
			new UnsupportedOperationException("readonly");
		
		if (m_merged)
			new UnsupportedOperationException("merged index, adding triples not allowed");
		
		String propsub = property + "__" + subject;
		String propobj = property + "__" + object;
		
		Document doc1 = new Document();
		doc1.add(new Field(Index.PS.getIndexField(), propsub, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc1.add(new Field(FIELD_OBJECT, object, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));

		Document doc2 = new Document();
		doc2.add(new Field(Index.PO.getIndexField(), propobj, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc2.add(new Field(FIELD_SUBJECT, subject, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
//		System.out.println(subject + " " + property + " " + object);
		
		try {
			m_writer.addDocument(doc1);
			m_writer.addDocument(doc2);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void flush() {
		try {
			if (!m_readonly)
				m_writer.flush();
			m_reader = m_reader.reopen();
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			if (!m_readonly)
				m_writer.close();
			m_searcher.close();
			m_reader.close();
			m_queryExecutor.shutdown();
			m_documentLoader.shutdown();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void optimize() {
		try {
			if (!m_readonly)
				m_writer.optimize();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void loadDocuments(GTable<String> table, List<Integer> docIds, Index index, String so) throws StorageException {
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			if (index == Index.PS || index == Index.SP) {
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
	}

	@SuppressWarnings("unchecked")
	public List<GTable<String>> getIndexTables(Index index, String property) throws IOException, StorageException {
		if (index == Index.OP || index == Index.SP)
			throw new UnsupportedOperationException("querying OP and SP indexes with only a property is not supported");
		
		long start = System.currentTimeMillis();
	
		PrefixQuery pq = new PrefixQuery(new Term(index.getIndexField(), property + "__"));
		String query = pq.toString();
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
		for (BooleanClause bc : bq.getClauses())
			queries.add((TermQuery)bc.getQuery());
		
		List<Object[]> dis = new ArrayList<Object[]>();
		for (TermQuery q : queries) {
			String term = q.getTerm().text();
			String so;
			if (index == Index.SP || index == Index.OP)
				so = term.substring(0, term.indexOf("__"));
			else
				so = term.substring(term.lastIndexOf("__") + 2);
			dis.add(new Object[] { so, getDocumentIds(q) });
		}
		
		List<GTable<String>> tables = new ArrayList<GTable<String>>();
		int docs = 0;
		int rows = 0;
		for (Object[] o : dis) {
			String so = (String)o[0];
			List<Integer> docIds = (List<Integer>)o[1];
			GTable<String> table = new GTable<String>("source", "target");
			loadDocuments(table, docIds, index, so);
			
			docs += docIds.size();
			rows += table.rowCount();
			
			tables.add(table);
		}

//		log.debug("q: " + query + " (" + docs + "/" + rows + ") {" + (System.currentTimeMillis() - start) + " ms}");
		return tables;
	}
	
	public GTable<String> getIndexTable(Index index, String property, String so) throws IOException, StorageException {
//		long start = System.currentTimeMillis();

		String indexString;
		if (index == Index.SP || index == Index.OP)
			indexString = so + "__" + property;
		else
			indexString = property + "__" + so;
		
		TermQuery tq = new TermQuery(new Term(index.getIndexField(), indexString));
			
		GTable<String> table = new GTable<String>("source", "target");
		
//		long ds = System.currentTimeMillis();
		List<Integer> docIds = getDocumentIds(tq);
//		ds = System.currentTimeMillis() - ds;
		
//		long dr = System.currentTimeMillis();
		loadDocuments(table, docIds, index, so);
//		dr = System.currentTimeMillis() - dr;
		
		if (index == Index.OP || index == Index.PO)
			table.setSortedColumn(0);
		else
			table.setSortedColumn(1);
		
//		log.debug("q: " + tq + " (" + docIds.size() + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) + " ms, " + ds + ", " + dr + "}");
		
		return table;
	}
	
	public void merge() throws IOException, StorageException {
		if (!m_readonly) {
			m_writer.optimize();
			m_writer.close();
		}
		
		IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(m_directory + "_merged"), true, new WhitespaceAnalyzer(), true);
		iw.setRAMBufferSizeMB(1024);
		iw.setInfoStream(System.out);
		
		log.debug(m_reader.maxDoc());
		
		TermEnum te = m_reader.terms();
		int total = 0;
		while (te.next())
			total++;
		te.close();
		
		log.debug("terms: " + total);
		
		int terms = 0;
		te = m_reader.terms();
		long ts = 0, tw = 0;
		while (te.next()) {
			Term t = te.term();
//			log.debug(t + " " + t.field());
			
			long start = System.currentTimeMillis();
			
			TermQuery tq = new TermQuery(t);
			List<Integer> docIds = getDocumentIds(tq);
			
			List<String> values = new ArrayList<String>();
			for (int docId : docIds) {
				Document doc = getDocument(docId);
				if (t.field().equals(Index.PO.getIndexField()))
					values.add(doc.getField(FIELD_SUBJECT).stringValue());
				else
					values.add(doc.getField(FIELD_OBJECT).stringValue());
			}
			
			Collections.sort(values);
			
			StringBuilder sb = new StringBuilder();
			for (String val : values)
				sb.append(val).append("\n");
			
			ts += System.currentTimeMillis() - start;
			start = System.currentTimeMillis();

			Document doc = new Document();
			if (t.field().equals(Index.PO.getIndexField())) {
				doc.add(new Field(Index.PO.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
//				doc.add(new Field(Index.OP.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(FIELD_SUBJECT, sb.toString(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
			}
			else {
				doc.add(new Field(Index.PS.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
//				doc.add(new Field(Index.SP.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(FIELD_OBJECT, sb.toString(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
			}
			iw.addDocument(doc);
			
			tw += System.currentTimeMillis() - start;
			
			terms++;
			if (terms % 5000 == 0) {
				log.debug(terms + "/" + total + " " + (ts / 5000) + " " + (tw / 5000));
				tw = ts = 0;
			}
		}
		log.debug("flushing and optimizing...");
		iw.flush();
		iw.optimize();
		
		iw.close();
		log.debug("done");
	}

	public void clearCaches() {
		m_docCache.clear();
	}

	public void warmUp(Set<String> queries) throws StorageException {
		for (String query : queries) {
			String[] t = query.split(" ", 2);
			Query q = new PrefixQuery(new Term(t[0], t[1]));
			List<Integer> docIds = getDocumentIds(q);
//			log.debug("warmup: " + q + " => " + docIds.size() + " doc ids");
		}
	}
}
