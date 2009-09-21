package edu.unika.aifb.graphindex.storage.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage.TriplesIterator;
import edu.unika.aifb.graphindex.util.StringSplitter;

public class LuceneExtendedIndexStorage implements IndexStorage {
	private interface DocumentIdCollector {
		public void docId(int docId) throws StorageException;
	}
	
	
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	private boolean m_readonly = true;
	private File m_directory;
	
	private static final String KEY_DELIM = Character.toString((char)31);
	
	private static final Logger log = Logger.getLogger(LuceneIndexStorage.class);

	public LuceneExtendedIndexStorage(File dir) {
		m_directory = dir;
	}
	
	public void initialize(boolean clean, boolean readonly) {
		m_readonly = readonly;
		
		try {
			if (!m_readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(Runtime.getRuntime().maxMemory() / 1000 / 1000 / 10);
				log.debug("IndexWriter ram buffer size set to " + m_writer.getRAMBufferSizeMB() + "MB");
			}
			
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		BooleanQuery.setMaxClauseCount(4194304);
	}
	
	public void close() throws StorageException {
			try {
				m_searcher.close();
				m_reader.close();
				if (!m_readonly)
					m_writer.close();
			} catch (CorruptIndexException e) {
				throw new StorageException(e);
			} catch (IOException e) {
				throw new StorageException(e);
			}
	}
	
	public void reopen() throws StorageException {
		try {
			m_searcher.close();
			m_reader.close();
	
			m_writer.flush();
	
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public String toIndexKey(String[] indexKeys) {
		StringBuilder sb = new StringBuilder();
		for (String s : indexKeys)
			sb.append(s).append(KEY_DELIM);
		return sb.toString();
	}

	public void addData(IndexDescription index, String[] indexKeys, Collection<String> values) throws StorageException {
		String indexKey = toIndexKey(indexKeys);
		
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

	public void addData(IndexDescription index, String[] indexKeys, List<String> values, boolean sort) throws StorageException {
		if (sort)
			Collections.sort(values);
		addData(index, indexKeys, values);
	}

	public void addData(IndexDescription index, String[] indexKeys, String value) throws StorageException {
		String indexKey = toIndexKey(indexKeys);
		
		Document doc = new Document();
		doc.add(new Field(index.getIndexFieldName(), indexKey, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
		doc.add(new Field(index.getValueFieldName(), value, Field.Store.YES, Field.Index.NO));
		
		try {
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
//		log.debug(q + "  " + docIds.size() + " docs");
		
		Collections.sort(docIds);
		
		return docIds;
	}
	
	private class Value<T> {
		T value;
	}
	private int collectDocumentIds(Query q, final DocumentIdCollector collector) throws StorageException {
		final Value<Integer> docs = new Value<Integer>();
		try {
			m_searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					try {
						collector.docId(docId);
					} catch (StorageException e) {
						e.printStackTrace();
					}
					docs.value++;
				}
			});
		} catch (IOException e) {
			throw new StorageException(e);
		}
		return docs.value;
	}
	
	private Document getDocument(int docId) throws StorageException {
		try {
			Document doc = m_reader.document(docId);
			return doc;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	private void loadDocuments(Table<String> table, List<Integer> docIds, IndexDescription index, int[] valueIdxs, String[] indexValues) throws StorageException {
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			String values = doc.getField(index.getValueFieldName()).stringValue();
			
			// hack if the value field is empty, mainly for triple data (instead of quads)
			if (values.length() == 0) {
				String[] row = new String [table.columnCount()];
				for (int i = 0; i < table.columnCount(); i++) {
					int idx = valueIdxs[i];
					
					if (idx == -1)
						row[i] = null;
					else
						row[i] = indexValues[idx];
				}
				
				table.addRow(row);
				
				return;
			}
			
			StringSplitter splitter = new StringSplitter(values, "\n");
			
			String s;
			while ((s = splitter.next()) != null) {
				String[] row = new String [table.columnCount()];
				
				for (int i = 0; i < table.columnCount(); i++) {
					int idx = valueIdxs[i];
					
					if (idx == -1)
						row[i] = s;
					else
						row[i] = indexValues[idx];
				}
				
				table.addRow(row);
 			}
		}		
	}
	
	private void loadDocuments(Table<String> table, List<Integer> docIds, IndexDescription index, int valueCol, String indexValue) throws StorageException {
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			if (valueCol == 1) {
				String objects = doc.getField(index.getValueFieldName()).stringValue();
				StringSplitter splitter = new StringSplitter(objects, "\n");
				
				String s;
				while ((s = splitter.next()) != null)
					table.addRow(new String[] { indexValue, s });
			}
			else {
				String subjects = doc.getField(index.getValueFieldName()).stringValue();
				StringSplitter splitter = new StringSplitter(subjects, "\n");
				
				String s;
				while ((s = splitter.next()) != null)
					table.addRow(new String[] { s, indexValue });
			}
		}
	}

	private List<String[]> loadDocument(int docId, IndexDescription index, DataField[] fields) throws StorageException {
		List<String[]> rows = new ArrayList<String[]>();
		Document doc = getDocument(docId);

		String[] stub = new String[fields.length];
		int multiValueFieldIdx = -1;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] == index.getValueField()) {
				stub[i] = null;
				multiValueFieldIdx = i;
			}
			else
				stub[i] = doc.getField(fields[i].toString()).stringValue();
		}
		
		StringSplitter splitter = new StringSplitter(doc.getField(index.getValueFieldName()).stringValue(), "\n");
		String s;
		while ((s = splitter.next()) != null) {
			String[] copy = new String[stub.length];
			System.arraycopy(stub, 0, copy, 0, stub.length);
			copy[multiValueFieldIdx] = s;
			rows.add(copy);
		}
		
		if (rows.size() == 0)
			rows.add(stub);
		
		return rows;
	}

	private String getIndexKey(String... indexFields) {
		StringBuilder sb = new StringBuilder();
		for (String idxField : indexFields)
			sb.append(idxField).append(KEY_DELIM);
		return sb.toString();
	}
	
	public String getDataItem(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException {
		if (indexFieldValues.length < index.getIndexFields().size())
			throw new UnsupportedOperationException("getDataItem supports only queries with one result document");
		
		String value = null;
		
		TermQuery q = new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFieldValues)));
		
		List<Integer> docIds = getDocumentIds(q);
		if (docIds.size() > 0) {
			Document doc = getDocument(docIds.get(0));
			value = doc.getField(index.getValueFieldName()).stringValue().trim();
		}

		return value;
	}

	public List<String> getDataList(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException {
		List<String> values = new ArrayList<String>();
		getData(index, field, values, indexFieldValues);
		return values;
	}

	public Set<String> getDataSet(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException {
		Set<String> values = new HashSet<String>();
		getData(index, field, values, indexFieldValues);
		return values;
	}
	
	private void getData(IndexDescription index, DataField field, Collection<String> values, String... indexFieldValues) throws StorageException {
		Table<String> table = getTable(index, new DataField[] { field }, indexFieldValues);
		for (String[] row : table)
			values.add(row[0]);
	}
	
	@SuppressWarnings("unchecked")
	public Table<String> getTable(final IndexDescription index, final DataField[] columns, String... indexFieldValues) throws StorageException {
		Query q;
		String indexKey = getIndexKey(indexFieldValues);
		if (indexFieldValues.length < index.getIndexFields().size()) 
			q = new PrefixQuery(new Term(index.getIndexFieldName(), indexKey));
		else
			q = new TermQuery(new Term(index.getIndexFieldName(), indexKey));
		
		final Table<String> t = new Table<String>(columns);
		
		int documents = collectDocumentIds(q, new DocumentIdCollector() {
			@Override
			public void docId(int docId) throws StorageException {
				t.addRows(loadDocument(docId, index, columns));
			}
		});
		
		
		List<String> cols = new ArrayList<String>();
		for (DataField df : columns)
			cols.add(df.toString());
		
		Table<String> table = new Table<String>(cols);

		boolean usesValueField = false;
		int[] valueIdxs = new int [columns.length];
		for (int i = 0; i < columns.length; i++) {
			DataField colField = columns[i];
			
			if (colField == index.getValueField()) {
				valueIdxs[i] = -1;
				usesValueField = true;
			}
			else {
				valueIdxs[i] = index.getIndexFieldPos(colField);
			}
		}
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		
		indexKey = getIndexKey(indexFieldValues);
		if (indexFieldValues.length < index.getIndexFields().size()) {
			PrefixQuery pq = new PrefixQuery(new Term(index.getIndexFieldName(), indexKey));
			try {
				BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
				if (!usesValueField) {
					for (BooleanClause bc : bq.getClauses()) {
						String term = ((TermQuery)bc.getQuery()).getTerm().text();
						String[] indexTerms = term.split(KEY_DELIM);
						String[] row = new String[table.columnCount()];
						for (int i = 0; i < row.length; i++)
							row[i] = indexTerms[valueIdxs[i]];
						table.addRow(row);
					}
				}
				else {
					for (BooleanClause bc : bq.getClauses())
						queries.add((TermQuery)bc.getQuery());
				}
			} catch (IOException e1) {
				throw new StorageException(e1);
			}
		}
		else {
			queries.add(new TermQuery(new Term(index.getIndexFieldName(), indexKey)));
		}
		
		if (!usesValueField)
			return table;
		
		List<Object[]> dis = new ArrayList<Object[]>();
		for (TermQuery tq : queries) {
			String term = tq.getTerm().text();
			String[] indexTerms = term.split(KEY_DELIM);
			
			dis.add(new Object[] { indexTerms, getDocumentIds(tq) });
		}

		
		for (Object[] o : dis) {
			loadDocuments(table, (List<Integer>)o[1], index, valueIdxs, (String[])o[0]);
		}

		return table;
	}
	
	public Table<String> getIndexTable(IndexDescription index, DataField col1, DataField col2, String... indexFields) throws StorageException {
		if (indexFields.length < index.getIndexFields().size())
			return getIndexTables(index, col1, col2, indexFields);

		Table<String> table = new Table<String>("source", "target");
		
		TermQuery q = new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		
		int valueCol = index.getValueField() == col1 ? 0 : 1;
		
		String indexValue = null;
		for (int i = 0; i < indexFields.length; i++) {
			DataField df = index.getIndexFields().get(i);
			if ((valueCol == 0 && col2 == df) || (valueCol == 1 && col1 == df)){
				indexValue = indexFields[i];
				break;
			}
		}
		
		List<Integer> docIds = getDocumentIds(q);
		
		if (docIds.size() > 0) {
			loadDocuments(table, docIds, index, valueCol, indexValue);
			
			if (index.getValueField() == col1)
				table.setSortedColumn(0);
			else
				table.setSortedColumn(1);
		}
			
		return table;
	}

	@SuppressWarnings("unchecked")
	private Table<String> getIndexTables(IndexDescription index, DataField col1, DataField col2, String... indexFields) throws StorageException {
		PrefixQuery pq = new PrefixQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		try {
			BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
			for (BooleanClause bc : bq.getClauses())
				queries.add((TermQuery)bc.getQuery());
		} catch (IOException e1) {
			throw new StorageException(e1);
		}
		
		int valueCol = index.getValueField() == col1 ? 0 : 1;

		int indexValue = 0;
		for (int i = 0; i < index.getIndexFields().size(); i++) {
			DataField df = index.getIndexFields().get(i);
			if ((valueCol == 0 && col2 == df) || (valueCol == 1 && col1 == df)){
				indexValue = i;
				break;
			}
		}

		List<Object[]> dis = new ArrayList<Object[]>();
		for (TermQuery q : queries) {
			String term = q.getTerm().text();
			String[] indexTerms = term.split(KEY_DELIM);
			dis.add(new Object[] { indexTerms[indexValue], getDocumentIds(q) });
		}
		
		Table<String> table = new Table<String>("source", "target");

		for (Object[] o : dis) {
			String so = (String)o[0];
			for (int docId : (List<Integer>)o[1]) {
				Document doc = getDocument(docId);
				
				if (index.getValueField() == col2) {
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
//		log.debug("q: " + query + " (" + x + ") {" + (System.currentTimeMillis() - start) + " ms}");
//		m_timings.end(Timings.DATA);
		return table;
	}
	
	public boolean hasValues(IndexDescription index, String... indexFields) throws StorageException {
		Query q = null;
		if (indexFields.length < index.getIndexFields().size())
			q = new PrefixQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		else
			q = new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		
		try {
			Hits hits = m_searcher.search(q);
			return hits.length() > 0;
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	public void mergeIndex(final IndexDescription index) throws StorageException {
		try {
			reopen();

			TermEnum te = m_reader.terms(new Term(index.getIndexFieldName(), ""));
			do {
				Term t = te.term();
				
				if (!t.field().equals(index.getIndexFieldName()))
					break;
				
				List<Integer> docIds = getDocumentIds(new TermQuery(t));
				if (docIds.size() == 1)
					continue;
				
				final TreeSet<String> values = new TreeSet<String>();
				int docs = collectDocumentIds(new TermQuery(t), new DocumentIdCollector() {
					@Override
					public void docId(int docId) throws StorageException {
						Document doc = getDocument(docId);
						values.add(doc.getField(index.getValueFieldName()).stringValue().trim());
					}
				});
				
				if (docs <= 1)
					continue;
				
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
		} catch (IOException e) {
			throw new StorageException(e);
		}

	}
	
	public void optimize() throws StorageException {
		if (!m_readonly) {
			try {
				m_writer.optimize();
				reopen();
			} catch (IOException e) {
				throw new StorageException(e);
			}
		}
	}

	public class TriplesIterator implements Iterator<String[]> {

		private IndexDescription m_index;
		private Iterator<TermQuery> m_queryIterator;
		private List<String> m_values;
		private Iterator<String> m_valueIterator;
		private int[] m_colIdx2TermIdx;
		private boolean m_usesValue = false;
		private String[] m_indexTerms;
		
		public TriplesIterator(IndexDescription index, List<TermQuery> queries) {
			this(index, queries, DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT);
		}
		
		public TriplesIterator(IndexDescription index, List<TermQuery> queries, DataField... columnFields) {
			m_index = index;
			m_queryIterator = queries.iterator();

			m_colIdx2TermIdx = new int [columnFields.length];
			for (int i = 0; i < columnFields.length; i++) {
				m_colIdx2TermIdx[i] = index.getIndexFieldPos(columnFields[i]);
				if (columnFields[i] == index.getValueField())
					m_usesValue = true;
			}
		}
		
		public boolean hasNext() {
			if (!m_usesValue && !m_queryIterator.hasNext())
				return false;
			
			if (m_usesValue && !m_queryIterator.hasNext() && m_valueIterator != null && !m_valueIterator.hasNext())
				return false;
			
			return true;
		}

		public String[] next() {
			if (m_indexTerms == null || m_values == null ||!m_valueIterator.hasNext()) {
				if (!m_queryIterator.hasNext())
					return null;

				TermQuery q = m_queryIterator.next();
				String term = q.getTerm().text();
				m_indexTerms = term.split(KEY_DELIM);
				
				if (m_indexTerms.length < m_index.getIndexFields().size()) {
					String s = "";
					for (String idxTerm : m_indexTerms)
						s += idxTerm + " ";
					log.debug(q + ", " + term + ", " + s);
				}
			}
			
			String[] row = new String [m_colIdx2TermIdx.length];

			if (!m_usesValue) {
				for (int i = 0; i < m_colIdx2TermIdx.length; i++)
					row[i] = m_indexTerms[m_colIdx2TermIdx[i]];
			}
			else {
				if (m_values == null || !m_valueIterator.hasNext()) {
					try {
						m_values = getDataList(m_index, m_index.getValueField(), m_indexTerms);
						m_valueIterator = m_values.iterator();
					} catch (StorageException e) {
						e.printStackTrace();
						return null;
					}
				}
				
				for (int i = 0; i < m_colIdx2TermIdx.length; i++) {
					if (m_colIdx2TermIdx[i] >= m_indexTerms.length)
						row[i] = m_valueIterator.next();
					else
						row[i] = m_indexTerms[m_colIdx2TermIdx[i]];
				}
			}
			
			return row;
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
		
	}
	
	@Override
	public Iterator<String[]> iterator(IndexDescription index, DataField[] columns, String... indexValues)	throws StorageException {
		List<TermQuery> queries = new ArrayList<TermQuery>();
		
		if (indexValues.length < index.getIndexFields().size()) {
			PrefixQuery q = new PrefixQuery(new Term(index.getIndexFieldName(), getIndexKey(indexValues)));
			try {
				BooleanQuery bq = (BooleanQuery)q.rewrite(m_reader);
				for (BooleanClause bc : bq.getClauses())
					queries.add((TermQuery)bc.getQuery());
			} catch (IOException e) {
				throw new StorageException(e);
			}
		}
		else
			queries.add(new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexValues))));
		
		return new TriplesIterator(index, queries, columns);
	}

	@Override
	public void mergeSingleIndex(IndexDescription index) throws StorageException {
		// TODO Auto-generated method stub
		
	}
}
