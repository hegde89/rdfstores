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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.LockObtainFailedException;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Util;

public class LuceneTest {

	private static IndexSearcher m_searcher;
	private static IndexReader m_reader;
	
	private interface DocumentIdCollector {
		public void collect(int docId);
	}
	
	private static void search(Query q, final DocumentIdCollector collector) throws StorageException {
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

	public static List<Integer> getDocumentIds(Query q) throws StorageException {
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
		
//		Collections.sort(docIds);
		
		return docIds;
	}

	private static void addToTable(GTable<String> table, Document doc, String allowedSubject) {
		String[] subjectStrings;
		String subjectString;
		subjectString = doc.getField("subject").stringValue();
		
		StringSplitter splitter = new StringSplitter(subjectString, "\n");
		
		String object = doc.getField("object").stringValue();

//		for (String s : subjectStrings) {
//			String[] t = s.split("\t");
//			table.addRow(new String [] { t[0], object });
//		}
//		log.debug(object);
//		for (String s : subjectStrings) {
		String s;
		while ((s = splitter.next()) != null) {
//			log.debug(s);
//			if (allowedSubject != null)
//				log.debug(s);
			if (allowedSubject == null || s.equals(allowedSubject))
				table.addRow(new String [] { s, object });
		}
	}
	
	private static Document getDocument(int docId) throws StorageException {
		try {
			Document doc = m_reader.document(docId);
			return doc;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public static void test1(List<Query> queries) throws StorageException {
		System.out.println("test1: retrieve doc ids and then load docs for each query");
		for (Query q : queries) {
			System.out.print(q + " ");
			GTable<String> table = new GTable<String>("source", "target");
			long start = System.currentTimeMillis();
			List<Integer> docIds = getDocumentIds(q);
			System.out.print(System.currentTimeMillis() - start + " ");
			
			start = System.currentTimeMillis();
			for (int id : docIds) {
				Document doc = getDocument(id);
//				addToTable(table, doc, null);
			}
			System.out.print(System.currentTimeMillis() - start + " ");
			System.out.print(docIds.size() + " ");
			System.out.println(table.rowCount());
		}
	}

	private static class Test1a implements Callable<GTable<String>> {
		private Query q;

		public Test1a(Query q) {
			this.q = q;
		}

		public GTable<String> call() throws Exception {
			System.out.println(q + " ");
			GTable<String> table = new GTable<String>("source", "target");
			long start = System.currentTimeMillis();
			List<Integer> docIds = getDocumentIds(q);
//			System.out.print(System.currentTimeMillis() - start + " ");
			
			start = System.currentTimeMillis();
			for (int id : docIds) {
				Document doc = getDocument(id);
				addToTable(table, doc, null);
			}
//			System.out.print(System.currentTimeMillis() - start + " ");
//			System.out.print(docIds.size() + " ");
//			System.out.println(table.rowCount());
			return table;
		}
	}
	public static void test1a(List<Query> queries) throws StorageException, InterruptedException, ExecutionException {
		System.out.println("test1a: retrieve doc ids and then load docs for each query with multiple threads");
		ExecutorService executor = Executors.newFixedThreadPool(1);
		ExecutorCompletionService<GTable<String>> cs = new ExecutorCompletionService<GTable<String>>(executor);
		for (Query q : queries) {
			cs.submit(new Test1a(q));
		}
		
		for (int i = 0; i < queries.size(); i++) {
			GTable<String> table = cs.take().get();
		}
		executor.shutdown();
	}

	public static void test2(List<Query> queries) throws StorageException {
		System.out.println("test2: retrieve doc ids, load each doc");
		for (Query q : queries) {
			System.out.print(q + " ");
			final GTable<String> table = new GTable<String>("source", "target");
			final long start = System.currentTimeMillis();
			search(q, new DocumentIdCollector() {
				public void collect(int docId) {
					Document doc;
					try {
						doc = getDocument(docId);
						addToTable(table, doc, null);
					} catch (StorageException e) {
					}
				}
			});
			System.out.println(" " + table.rowCount());
		}
	}
	
	private static class Test3 implements Callable<GTable<String>> {

		private List<Integer> m_docIds;

		public Test3(List<Integer> docIds) {
			m_docIds = docIds;
		}
		
		public GTable<String> call() throws Exception {
			GTable<String> table = new GTable<String>("source", "target");
			for (int id : m_docIds) {
				Document doc = getDocument(id);
				addToTable(table, doc, null);
			}
			System.out.println(table.rowCount());
			return table;
		}
		
	}
	
	public static void test3(List<Query> queries) throws StorageException, InterruptedException, ExecutionException {
		System.out.println("test3: retrieve doc ids, separate thread for loading docs for each query");
		ExecutorService executor = Executors.newFixedThreadPool(1);
		ExecutorCompletionService<GTable<String>> cs = new ExecutorCompletionService<GTable<String>>(executor);
		for (Query q : queries) {
			System.out.print(q + " ");
			long start = System.currentTimeMillis();
			List<Integer> docIds = getDocumentIds(q);
			System.out.print(docIds.size() + " ");
			System.out.println(System.currentTimeMillis() - start);
			
			cs.submit(new Test3(docIds));
		}
		
		for (int i = 0; i < queries.size(); i++) {
			GTable<String> res = cs.take().get();
		}
		executor.shutdown();
	}

	public static void test4(List<Query> queries) throws StorageException {
		System.out.println("test4: retrieve doc ids for all queries, then load");
		List<Integer> docIds = new ArrayList<Integer>();
		for (Query q : queries) {
			System.out.println(q);
			docIds.addAll(getDocumentIds(q));
			
		}
		Collections.sort(docIds);
		System.out.println(docIds.size());
		for (int id : docIds) {
			Document doc = getDocument(id);
//			addToTable(table, doc, null);
		}
	}
	
	public static class Test5 implements Runnable {
		private BlockingQueue<Integer> m_bc;
		private List<Document> m_docs = new LinkedList<Document>();
		private int m_size = -1;

		public Test5(BlockingQueue<Integer> bc) {
			m_bc = bc;
		}
		
		public List<Document> getDocuments() {
			return m_docs;
		}
		
		synchronized public void setSize(int size) {
			m_size = size;
		}

		public void run() {
			try {
				int x = 0;
				while (m_size == -1 || x < m_size) {
					int docId = m_bc.take();
					m_docs.add(getDocument(docId));
					x++;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void test5(List<Query> queries) throws StorageException, InterruptedException, ExecutionException {
		System.out.println("test5: separate thread for loading docs, using blockingqueue for doc ids");
		BlockingQueue<Integer> bc = new LinkedBlockingQueue<Integer>();
		Test5 t5 = new Test5(bc);

		Thread t = new Thread(t5);
		t.start();
		
		int x = 0;
		for (Query q : queries) {
			System.out.print(q + " ");
			long start = System.currentTimeMillis();
			List<Integer> docIds = getDocumentIds(q);
			x += docIds.size();
			System.out.print(docIds.size() + " ");
			System.out.println(System.currentTimeMillis() - start);

			bc.addAll(docIds);
		}
		t5.setSize(x);
		t.join();
		
		System.out.println(t5.getDocuments().size());
	}

	public static void test5a(List<Query> queries) throws StorageException, InterruptedException, ExecutionException {
		System.out.println("test5a: separate thread for loading docs, using blockingqueue for doc ids");
		final BlockingQueue<Integer> bc = new LinkedBlockingQueue<Integer>();
		Test5 t5 = new Test5(bc);

		Thread t = new Thread(t5);
		t.start();
		
		final Util.Counter c = new Util.Counter();
		for (Query q : queries) {
			System.out.println(q + " ");
			long start = System.currentTimeMillis();
			search(q, new DocumentIdCollector() {

				public void collect(int docId) {
					bc.add(docId);
					c.val++;
				}
				
			});
		}
		t5.setSize(c.val);
		t.join();
		
		System.out.println(t5.getDocuments().size());
	}

	public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException, StorageException, InterruptedException, ExecutionException {
		m_reader = IndexReader.open("/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/lubm50/index");
		m_searcher = new IndexSearcher(m_reader);
		BooleanQuery.setMaxClauseCount(2000000);
		List<Query> queries = new ArrayList<Query>();
		BufferedReader in = new BufferedReader(new FileReader("/Users/gl/Studium/diplomarbeit/graphindex evaluation/lq7storagequeries.txt"));
		String input;
		while ((input = in.readLine()) != null) {
			queries.add(input.indexOf("*") < 0 ? new TermQuery(new Term("ext", input)) : new PrefixQuery(new Term("ext", input.replaceAll("\\*", ""))));
		}
		
		long start = System.currentTimeMillis();
//		test2(queries);
//		System.out.println("t: " + (System.currentTimeMillis() - start));
//
//		start = System.currentTimeMillis();
//		test3(queries);
//		System.out.println("t: " + (System.currentTimeMillis() - start));
//		
//		start = System.currentTimeMillis();
//		test4(queries);
//		System.out.println("t: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		test1(queries);
		System.out.println("t: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		test1a(queries);
		System.out.println("t: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		test5(queries);
		System.out.println("t: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		test5a(queries);
		System.out.println("t: " + (System.currentTimeMillis() - start));
	}
}
