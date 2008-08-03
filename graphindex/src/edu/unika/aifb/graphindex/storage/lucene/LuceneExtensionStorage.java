package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.storage.AbstractExtensionStorage;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;

public class LuceneExtensionStorage extends AbstractExtensionStorage {
	
	private String m_directory;
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	private final String FIELD_EXT = "ext";
	private final String FIELD_SUBJECT = "subject";
	private final String FIELD_PROPERTY = "property";
	private final String FIELD_OBJECT = "object";
	private final String FIELD_TYPE = "type";
	private final String FIELD_VAL = "value";
	private final String TYPE_EXTLIST = "extension_list";
	private final String EXT_PATH_SEP = "//";
	private final static Logger log = Logger.getLogger(LuceneExtensionStorage.class);
	
	public LuceneExtensionStorage(String directory) {
		m_directory = directory;
	}
	
	public void initialize(boolean clean) {
		try {
			m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			m_writer.close();
			m_searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void finishBulkUpdate() throws StorageException {
		try {
			m_writer.flush();
			reopen();
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
			Hits hits = m_searcher.search(new TermQuery(new Term(FIELD_TYPE, TYPE_EXTLIST)));
			
			Document doc = hits.doc(0);
			
			if (doc != null) {
				String uriString = doc.getField(FIELD_VAL).stringValue();
				for (String uri :uriString.split("\n")) {
					uris.add(uri);
				}
			}
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return uris;
	}
	
	public void saveExtensionList(Set<String> uris) throws StorageException {
		if (bulkUpdating())
			throw new StorageException("in bulk mode");
		
		try {
			m_writer.deleteDocuments(new Term(FIELD_TYPE, TYPE_EXTLIST));
			
			String uriString = "";
			String addCR = "";
			for (String uri : uris) {
				uriString += addCR + uri;
				addCR = "\n";
			}
			
			Document doc = new Document();
			doc.add(new Field(FIELD_TYPE, TYPE_EXTLIST, Field.Store.NO, Field.Index.UN_TOKENIZED));
			doc.add(new Field(FIELD_VAL, uriString, Field.Store.YES, Field.Index.NO));
			
			m_writer.addDocument(doc);
			
			m_writer.flush();
			reopen();
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
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
		log.debug("query: " + q);
		Set<Triple> triples = new HashSet<Triple>();
		
		// TODO lucene doc says that retrieving all documents matching a query should be done with HitCollector
		Hits hits = m_searcher.search(q);
		for (Iterator i = hits.iterator(); i.hasNext(); ) {
			Hit hit = (Hit)i.next();
			Document doc = hit.getDocument();
			Triple t = new Triple(doc.getField(FIELD_SUBJECT).stringValue(), doc.getField(FIELD_PROPERTY).stringValue(), doc.getField(FIELD_OBJECT).stringValue());
			triples.add(t);
		}
		
		return triples;
	}
	
	public Set<Triple> loadData(String extUri) throws IOException {
		PrefixQuery q = new PrefixQuery(getTerm(extUri));
		return executeQuery(q);
	}
	
	public Set<Triple> loadData(String extUri, String propertyUri) throws IOException {
		PrefixQuery q = new PrefixQuery(getTerm(extUri, propertyUri));
		return executeQuery(q);
	}

	public Set<Triple> loadData(String extUri, String propertyUri, String object) throws IOException {
		TermQuery q = new TermQuery(getTerm(extUri, propertyUri, object));
		return executeQuery(q);
	}
	
	private Document createDocument(String extUri, Triple t) {
		Document doc = new Document();
		
		doc.add(new Field(FIELD_EXT, getPath(extUri, t.getProperty(), t.getObject()), Field.Store.NO, Field.Index.TOKENIZED));
		doc.add(new Field(FIELD_SUBJECT, t.getSubject(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(FIELD_PROPERTY, t.getProperty(), Field.Store.YES, Field.Index.NO));
		doc.add(new Field(FIELD_OBJECT, t.getObject(), Field.Store.YES, Field.Index.NO));
		
		return doc;
	}
	
	public void saveData(String extUri, Triple triple) {
		try {
			Document doc = createDocument(extUri, triple);
			m_writer.addDocument(doc);
			
			if (!bulkUpdating()) {
				m_writer.flush();
				reopen();
			}
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void saveData(String extUri, Set<Triple> triples) {
		try {
			for (Triple t : triples) {
				Document doc = createDocument(extUri, t);
				m_writer.addDocument(doc);
			}
			
			if (!bulkUpdating()) {
				m_writer.flush();
				reopen();
			}
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void deleteDataByPath(String queryPath) throws IOException {
		List<Term> terms = new ArrayList<Term>();
		
		PrefixQuery q = new PrefixQuery(getTerm(queryPath));
		BooleanQuery bq = (BooleanQuery)q.rewrite(m_reader);
		
		for (BooleanClause bc : bq.getClauses())
			terms.add(((TermQuery)bc.getQuery()).getTerm());
		
		m_writer.deleteDocuments(terms.toArray(new Term[]{}));
		if (!bulkUpdating()) {
			m_writer.flush();
			reopen();
		}
	}
	
	public void deleteData(String extUri) throws IOException {
		deleteDataByPath(getPath(extUri));
	}
}
