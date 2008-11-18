package edu.unika.aifb.graphindex.vp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
	
	private static final String FIELD_SUBJECT = "subject";
	private static final String FIELD_PROPERTY = "property";
	private static final String FIELD_OBJECT = "object";
	private static final String FIELD_PROPOBJ = "propobj";
	private static final String FIELD_PROPSUB = "propsub";
	
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
		
		public DocumentLoader(List<Integer> docIds) {
			this.docIds = docIds;
		}
		
		public GTable<String> call() throws Exception {
			GTable<String> table = new GTable<String>("source", "target");
			try {
				for (int docId : docIds) {
					Document doc = getDocument(docId);
					addToTable(table, doc);
				}
			} catch (StorageException e) {
				e.printStackTrace();
			}
			
			return table;
		}
		
	}
	
	public LuceneStorage(String directory) {
		m_directory = directory;
		
		m_queryExecutor = Executors.newFixedThreadPool(2);
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

	private void addToTable(GTable<String> table, Document doc) {
		String subject = doc.getField(FIELD_SUBJECT).stringValue();
		String object = doc.getField(FIELD_OBJECT).stringValue();
		
		String[] row = new String[] { subject, object };

		table.addRow(row);
	}

	public void addTriple(String subject, String property, String object) {
		if (m_readonly)
			new UnsupportedOperationException("readonly");
		
		String propsub = property + "__" + subject;
		String propobj = property + "__" + object;
		
		Document doc1 = new Document();
		doc1.add(new Field(FIELD_PROPSUB, propsub, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc1.add(new Field(FIELD_OBJECT, object, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));

		Document doc2 = new Document();
		doc2.add(new Field(FIELD_PROPOBJ, propobj, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
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
	
	public GTable<String> getTable(String subject, String property, String object) {
		long start = System.currentTimeMillis();
		BooleanQuery bq = new BooleanQuery(true);
		
		if (property != null) {
			bq.add(new TermQuery(new Term(FIELD_PROPERTY, property)), BooleanClause.Occur.MUST);
		}
		
		if (subject != null) {
			bq.add(new TermQuery(new Term(FIELD_SUBJECT, subject)), BooleanClause.Occur.MUST);
		}
		
		if (object != null) {
			bq.add(new TermQuery(new Term(FIELD_OBJECT, object)), BooleanClause.Occur.MUST);
		}
		try {
			long ds = System.currentTimeMillis();
			Future<List<Integer>> f1 = m_queryExecutor.submit(new QueryExecutor(bq));
			List<Integer> docIds = f1.get();
			ds = System.currentTimeMillis() - ds;
			
			long dr = System.currentTimeMillis();
			Future<GTable<String>> f2 = m_documentLoader.submit(new DocumentLoader(docIds));
			GTable<String> table = f2.get();
			dr = System.currentTimeMillis() - dr;
			
			log.debug("q: " + bq + " (" + docIds.size() + "/" + table.rowCount() + ") {" + (System.currentTimeMillis() - start) / 1000.0 + " ms, " + ds + ", " + dr + "}");
			return table;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return new GTable<String>("source", "target");
	}
	
	public void merge() throws IOException, StorageException {
		TermEnum te = m_reader.terms();
		log.debug(m_reader.maxDoc());
		int terms = 0;
		while (te.next()) {
			Term t = te.term();
//			log.debug(t + " " + t.field());
			TermQuery tq = new TermQuery(t);
			List<Integer> docIds = getDocumentIds(tq);
			String val = "";
			for (int docId : docIds) {
				Document doc = getDocument(docId);
				if (t.field().equals(FIELD_PROPOBJ))
					val += doc.getField(FIELD_SUBJECT).stringValue() + "\n";
				else
					val += doc.getField(FIELD_OBJECT).stringValue() + "\n";
			}
			m_writer.deleteDocuments(t);
			
			Document doc = new Document();
			if (t.field().equals(FIELD_PROPOBJ)) {
				doc.add(new Field(FIELD_PROPOBJ, t.field(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(FIELD_SUBJECT, val, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
			}
			else {
				doc.add(new Field(FIELD_PROPSUB, t.field(), Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
				doc.add(new Field(FIELD_OBJECT, val, Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
			}
			m_writer.addDocument(doc);
			
			terms++;
			if (terms % 50000 == 0)
				log.debug(" " + terms);
		}
		
		m_writer.flush();
	}
}
