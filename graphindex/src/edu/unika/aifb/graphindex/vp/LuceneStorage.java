package edu.unika.aifb.graphindex.vp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.StorageException;
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
					
					if (m_merged) {
						if (index == Index.PS || index == Index.SP) {
							String objects = doc.getField(FIELD_OBJECT).stringValue();
							StringSplitter splitter = new StringSplitter(objects, "\n");
							
							String s;
							while ((s = splitter.next()) != null)
								table.addRow(new String[] { so, s });
						}
						else {
							String subjects = doc.getField(FIELD_SUBJECT).stringValue();
							StringSplitter splitter = new StringSplitter(subjects, "\n");
							
							String s;
							while ((s = splitter.next()) != null)
								table.addRow(new String[] { s, so });
						}
					}
					else {
						String subject, object;
						if (index == Index.PS) {
							subject = so;
							object = doc.getField(FIELD_OBJECT).stringValue();
						}
						else {
							subject = doc.getField(FIELD_SUBJECT).stringValue();
							object = so;
						}
						table.addRow(new String[] { subject, object });
					}
				}
			} catch (StorageException e) {
				e.printStackTrace();
			}
			
			return table;
		}
		
	}
	
	public LuceneStorage(String directory) {
		m_directory = directory;
		
		m_queryExecutor = Executors.newFixedThreadPool(1);
		m_documentLoader = Executors.newFixedThreadPool(1);
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
			return  m_reader.document(docId);
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
	
	public List<GTable<String>> getIndexTables(Index index, String property) throws IOException {
		if (index == Index.OP || index == Index.SP)
			throw new UnsupportedOperationException("querying OP and SP indexes with only a property is not supported");
		
		long start = System.currentTimeMillis();
	
		PrefixQuery pq = new PrefixQuery(new Term(index.getIndexField(), property + "__"));
		String query = pq.toString();
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
		for (BooleanClause bc : bq.getClauses())
			queries.add((TermQuery)bc.getQuery());
		
		long dq = System.currentTimeMillis() - start;

		List<GTable<String>> tables = new ArrayList<GTable<String>>();
		long ds = 0, dr = 0;
		int docs = 0;
		int rows = 0;
		for (TermQuery q : queries) {
			String term = q.getTerm().text();
			String so;
			if (index == Index.SP || index == Index.OP)
				so = term.substring(0, term.indexOf("__"));
			else
				so = term.substring(term.lastIndexOf("__") + 2);

			try {
				long s2 = System.currentTimeMillis();
				Future<List<Integer>> future1 = m_queryExecutor.submit(new QueryExecutor(q));
				List<Integer> docIds = future1.get();
				ds += System.currentTimeMillis() - s2;
				
				docs += docIds.size();
				
				s2 = System.currentTimeMillis();
				Future<GTable<String>> future2 = m_documentLoader.submit(new DocumentLoader(docIds, index, so));
				GTable<String> table = future2.get();
				tables.add(table);
				dr += System.currentTimeMillis() - s2;
				
				rows += table.rowCount();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		log.debug("q: " + query + " (" + docs + "/" + rows + ") {" + (System.currentTimeMillis() - start) + " ms, " + dq + ", " + ds + ", " + dr + "}");
		return tables;
	}
	
	public GTable<String> getIndexTable(Index index, String property, String so) throws IOException {
		long start = System.currentTimeMillis();

		String indexString;
		if (index == Index.SP || index == Index.OP)
			indexString = so + "__" + property;
		else
			indexString = property + "__" + so;
		
		TermQuery tq = new TermQuery(new Term(index.getIndexField(), indexString));
			
		GTable<String> table = new GTable<String>("source", "target");
		int docs = 0;
		
		long ds = System.currentTimeMillis();
		try {
			
			Future<List<Integer>> future1 = m_queryExecutor.submit(new QueryExecutor(tq));
			List<Integer> docIds = future1.get();
			ds = System.currentTimeMillis() - ds;
			
			docs += docIds.size();
			
			long dr = System.currentTimeMillis();
			Future<GTable<String>> future2 = m_documentLoader.submit(new DocumentLoader(docIds, index, so));
			table = future2.get();
			dr = System.currentTimeMillis() - dr;
			
			if (index == Index.OP || index == Index.PO)
				table.setSortedColumn(0);
			else
				table.setSortedColumn(1);
			
			log.debug("q: " + tq + " (" + docs + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) + " ms, " + ds + ", " + dr + "}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return table;
	}
	
	public GTable<String> getTable(String subject, String property, String object) throws IOException {
		long start = System.currentTimeMillis();
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		Query origQuery = null;
		Index index = null;
		
		long dq = System.currentTimeMillis();
		String query = property;
		if (object == null && subject == null) {
			index = Index.PO;
			origQuery = new PrefixQuery(new Term(Index.PO.getIndexField(), query));
			BooleanQuery bq = (BooleanQuery)origQuery.rewrite(m_reader);
			for (BooleanClause bc : bq.getClauses())
				queries.add((TermQuery)bc.getQuery());
		}
		else if (object == null) {
			index = Index.PS;
			query += "__" + subject;
			origQuery = new TermQuery(new Term(Index.PS.getIndexField(), query));
			queries.add((TermQuery)origQuery);
		}
		else if (subject == null) {
			index = Index.PO;
			query += "__" + object;
			origQuery = new TermQuery(new Term(Index.PO.getIndexField(), query));
			queries.add((TermQuery)origQuery);
		}
		dq = System.currentTimeMillis() - dq;
		
		GTable<String> table = new GTable<String>("source", "target");
		try {
			
			long ds = 0, dr = 0;
			int docs = 0;
			for (TermQuery q : queries) {
				long s2 = System.currentTimeMillis();
				String val = null;
				if (subject == null && object == null) {
					String term = q.getTerm().text();
					val = term.substring(term.lastIndexOf("__") + 2);
				}
				else if (object == null) {
					val = subject;
				}
				else if (subject == null) {
					val = object;
				}
				Future<List<Integer>> f1 = m_queryExecutor.submit(new QueryExecutor(q));
				List<Integer> docIds = f1.get();
				ds += System.currentTimeMillis() - s2;
				
				docs += docIds.size();
				
				s2 = System.currentTimeMillis();
				Future<GTable<String>> f2 = m_documentLoader.submit(new DocumentLoader(docIds, index, val));
				table.addRows(f2.get().getRows());
				dr += System.currentTimeMillis() - s2;
			}
			
			log.debug("q: " + origQuery + " (" + docs + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) + " ms, " + dq + ", " + ds + ", " + dr + "}");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
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
				doc.add(new Field(Index.OP.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(FIELD_SUBJECT, sb.toString(), Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
			}
			else {
				doc.add(new Field(Index.PS.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(Index.SP.getIndexField(), t.text(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
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
}
