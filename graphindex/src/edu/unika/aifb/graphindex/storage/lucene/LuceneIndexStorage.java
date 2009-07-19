package edu.unika.aifb.graphindex.storage.lucene;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Timings;

public class LuceneIndexStorage implements IndexStorage {
	
	private IndexWriter m_writer;
	private IndexReader m_reader;
	private IndexSearcher m_searcher;
	
	private boolean m_readonly = true;
	private File m_directory;
	
	private static final String KEY_DELIM = "__";
	
	private static final Logger log = Logger.getLogger(LuceneIndexStorage.class);

	public LuceneIndexStorage(File dir) {
		m_directory = dir;
	}
	
	public void initialize(boolean clean, boolean readonly) {
		m_readonly = readonly;
		
		try {
			if (!m_readonly) {
				m_writer = new IndexWriter(FSDirectory.getDirectory(m_directory), true, new WhitespaceAnalyzer(), clean);
				m_writer.setRAMBufferSizeMB(128);
			}
			
			m_reader = IndexReader.open(m_directory);
			m_searcher = new IndexSearcher(m_reader);
			
			
//			m_dataSetCache = new LRUCache<String,Set<String>>(50000);
//			m_dataListCache = new LRUCache<String,List<String>>(50000);
//			m_dataItemCache = new LRUCache<String,String>(50000);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		BooleanQuery.setMaxClauseCount(1048576);
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
	
	private void loadDocuments(GTable<String> table, List<Integer> docIds, IndexDescription index, int[] valueIdxs, String[] indexValues) throws StorageException {
		for (int docId : docIds) {
			Document doc = getDocument(docId);
			
			String values = doc.getField(index.getValueFieldName()).stringValue();
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
	
	private void loadDocuments(GTable<String> table, List<Integer> docIds, IndexDescription index, int valueCol, String indexValue) throws StorageException {
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

	private List<String> loadDocument(int docId, IndexDescription index) throws StorageException {
		List<String> values = new ArrayList<String>(100);
		Document doc = getDocument(docId);

		StringSplitter splitter = new StringSplitter(doc.getField(index.getValueFieldName()).stringValue(), "\n");
		String s;
		while ((s = splitter.next()) != null)
			values.add(s);
		return values;
	}

	private String getIndexKey(String... indexFields) {
		StringBuilder sb = new StringBuilder();
		for (String idxField : indexFields)
			sb.append(idxField).append(KEY_DELIM);
		return sb.toString();
	}
	
	private String getIndexKey(IndexDescription index, Map<DataField,String> indexValues) {
		String[] indexFields = new String[indexValues.size()];
		for (int i = 0; i < indexFields.length; i++) {
			indexFields[i] = indexValues.get(index.getIndexFields().get(i));
		}
		return getIndexKey(indexFields);
	}

	public String getDataItem(IndexDescription index, String... indexFields) throws StorageException {
		if (indexFields.length < index.getIndexFields().size())
			throw new UnsupportedOperationException("getDataItem supports only queries with one result document");
		
		String value = null;
		
		TermQuery q = new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		
		List<Integer> docIds = getDocumentIds(q);
		if (docIds.size() > 0) {
			Document doc = getDocument(docIds.get(0));
			value = doc.getField(index.getValueFieldName()).stringValue().trim();
		}

		return value;
	}

	public List<String> getDataList(IndexDescription index, String... indexFields) throws StorageException {
		List<String> values = new ArrayList<String>();
		getData(index, values, indexFields);
		return values;
	}

	public Set<String> getDataSet(IndexDescription index, String... indexFields) throws StorageException {
		Set<String> values = new HashSet<String>();
		getData(index, values, indexFields);
		return values;
	}
	
	private void getData(IndexDescription index, Collection<String> values, String... indexFields) throws StorageException {
		Query q;
		if (indexFields.length == index.getIndexFields().size())
			q = new TermQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		else
			q = new PrefixQuery(new Term(index.getIndexFieldName(), getIndexKey(indexFields)));
		
		List<Integer> docIds = getDocumentIds(q);
		for (int docId : docIds)
			values.addAll(loadDocument(docId, index));
	}

	@SuppressWarnings("unchecked")
	public GTable<String> getTable(IndexDescription index, DataField[] columns, Map<DataField,String> indexValues) throws StorageException {
		List<String> cols = new ArrayList<String>();
		for (DataField df : columns)
			cols.add(df.toString());
		
		GTable<String> table = new GTable<String>(cols);

		int[] valueIdxs = new int [columns.length];
		
		for (int i = 0; i < columns.length; i++) {
			DataField colField = columns[i];
			
			if (colField == index.getValueField())
				valueIdxs[i] = -1;
			else
				valueIdxs[i] = index.getIndexFieldPos(colField);
		}
		
		List<TermQuery> queries = new ArrayList<TermQuery>();
		
		String indexKey = getIndexKey(index, indexValues);
		if (indexValues.size() < index.getIndexFields().size()) {
			PrefixQuery pq = new PrefixQuery(new Term(index.getIndexFieldName(), indexKey));
			try {
				BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
				for (BooleanClause bc : bq.getClauses())
					queries.add((TermQuery)bc.getQuery());
			} catch (IOException e1) {
				throw new StorageException(e1);
			}
		}
		else {
			queries.add(new TermQuery(new Term(index.getIndexFieldName(), indexKey)));
		}
		
		List<Object[]> dis = new ArrayList<Object[]>();
		for (TermQuery q : queries) {
			String term = q.getTerm().text();
			String[] indexTerms = term.split(KEY_DELIM);
			
			dis.add(new Object[] { indexTerms, getDocumentIds(q) });
		}

		
		for (Object[] o : dis) {
			loadDocuments(table, (List<Integer>)o[1], index, valueIdxs, (String[])o[0]);
		}

		return table;
	}
	
	public GTable<String> getIndexTable(IndexDescription index, DataField col1, DataField col2, String... indexFields) throws StorageException {
		if (indexFields.length < index.getIndexFields().size())
			return getIndexTables(index, col1, col2, indexFields);

		GTable<String> table = new GTable<String>("source", "target");
		
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

	private GTable<String> getIndexTables(IndexDescription index, DataField col1, DataField col2, String... indexFields) throws StorageException {
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
		
		GTable<String> table = new GTable<String>("source", "target");

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

	public void mergeIndex(IndexDescription index) throws StorageException {
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
		private int m_keyValueIdx, m_propertyValueIdx;
		private String m_currentProperty, m_currentKeyValue;
		
		public TriplesIterator(IndexDescription index, List<TermQuery> queries) {
			m_index = index;
			m_queryIterator = queries.iterator();

			if (index.getValueField() == DataField.SUBJECT)
				m_keyValueIdx = index.getIndexFieldPos(DataField.OBJECT);
			else
				m_keyValueIdx = index.getIndexFieldPos(DataField.SUBJECT);
			m_propertyValueIdx = index.getIndexFieldPos(DataField.PROPERTY);
		}
		
		public boolean hasNext() {
			if (!m_queryIterator.hasNext() && !m_valueIterator.hasNext())
				return false;
			return true;
		}

		public String[] next() {
			if (m_values == null || !m_valueIterator.hasNext()) {
				if (!m_queryIterator.hasNext())
					return null;
				
				TermQuery q = m_queryIterator.next();
				String term = q.getTerm().text();
				String[] indexTerms = term.split(KEY_DELIM);
				
				m_currentKeyValue = indexTerms[m_keyValueIdx];
				m_currentProperty = indexTerms[m_propertyValueIdx];
				
				try {
					m_values = getDataList(m_index, indexTerms);
					m_valueIterator = m_values.iterator();
				} catch (StorageException e) {
					e.printStackTrace();
					return null;
				}
			}
			
			if (m_index.getValueField() == DataField.SUBJECT)
				return new String[] { m_valueIterator.next(), m_currentProperty, m_currentKeyValue };
			else
				return new String[] { m_currentKeyValue, m_currentProperty, m_valueIterator.next() };
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
		
	}
	
	public Iterator<String[]> iterator(IndexDescription index, String property) throws StorageException {
		PrefixQuery q = new PrefixQuery(new Term(index.getIndexFieldName(), getIndexKey(property)));

		List<TermQuery> queries = new ArrayList<TermQuery>();
		try {
			BooleanQuery bq = (BooleanQuery)q.rewrite(m_reader);
			for (BooleanClause bc : bq.getClauses())
				queries.add((TermQuery)bc.getQuery());
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return new TriplesIterator(index, queries);
	}

}
