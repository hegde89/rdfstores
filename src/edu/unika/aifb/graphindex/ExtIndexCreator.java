package edu.unika.aifb.graphindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Util;

public class ExtIndexCreator {
	private static IndexSearcher is;
	
	public static List<Integer> getDocumentIds(Query q) throws StorageException {
		final List<Integer> docIds = new ArrayList<Integer>();
		try {
			is.search(q, new HitCollector() {
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
	
	public static void updateIndex(String dir) throws IOException, StorageException {
		IndexReader ir = IndexReader.open(dir);
		is = new IndexSearcher(ir);
		int x = 0;
		
		List<String[]> entries = new ArrayList<String[]>();
		
		TermEnum te = ir.terms();
		while (te.next()) {
			Term t = te.term();
			if (!t.field().equals("oe"))
				continue;
			String object = t.text();

			String ext = null;
			Set<String> exts = new HashSet<String>();
			TermQuery tq = new TermQuery(t);
			List<Integer> docIds = getDocumentIds(tq);
			for (int id : docIds) {
				Document doc = is.doc(id);
				String subjectString = doc.getField("e").stringValue();
				StringSplitter splitter = new StringSplitter(subjectString, "\n");
				
				String s;
				while ((s = splitter.next()) != null) {
					exts.add(s);
					ext = s;
				}
			}
			
			if (exts.size() != 1)
				System.out.println("error");

			entries.add(new String[] { object, ext });
//			System.out.println(object + " " + ext);
			
			x++;
			
			if (x % 100000 == 0)
				System.out.println(x + " " + Util.memory());
		}
		
		is.close();
		ir.close();
		
		System.out.println(entries.size());
		
		System.out.println("writing new index...");
		x = 0;
		IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(dir), true, new WhitespaceAnalyzer(), false);
		for (String[] e : entries) {
			String object = e[0];
			String ext = e[1];
			
			iw.deleteDocuments(new Term("oe", object));
			
			Document doc = new Document();
			doc.add(new Field("oe", object + "__" + ext, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
			
			iw.addDocument(doc);
			
			x++;
			
			if (x % 100000 == 0)
				System.out.println(x + "/" + entries.size() + " objects");
		}

		System.out.println("optimizing...");
		iw.optimize();
		iw.close();
		System.out.println("done");
	}
	
	public static void main(String[] args) throws CorruptIndexException, IOException, StorageException {
		String dir = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/l50/index";
		
		if (args.length > 0)
			dir = args[0];
		
		IndexReader ir = IndexReader.open(dir);
		is = new IndexSearcher(ir);
		
		Map<String,Set<String>> object2ext = new HashMap<String,Set<String>>();
		Map<String,Set<String>> subject2ext = new HashMap<String,Set<String>>();
		
		System.out.println("reading terms");
		int x = 0;
		TermEnum te = ir.terms();
		while (te.next()) {
			Term t = te.term();
			if (!t.field().equals("epo"))
				continue;
			System.out.println(t.text());
			String[] path = t.text().split("__");
			String ext = path[0];
			String object = path[2];
			
			List<String> subjects = new ArrayList<String>();
			
			TermQuery tq = new TermQuery(t);
			List<Integer> docIds = getDocumentIds(tq);
			for (int id : docIds) {
				Document doc = is.doc(id);
				String subjectString = doc.getField("s").stringValue();
				StringSplitter splitter = new StringSplitter(subjectString, "\n");
				
				String s;
				while ((s = splitter.next()) != null)
					subjects.add(s);
			}
			
			Set<String> exts = object2ext.get(object);
			if (exts == null) {
				exts = new HashSet<String>();
				object2ext.put(object, exts);
			}
			exts.add(ext);

			for (String subject : subjects) {
				exts = subject2ext.get(subject);
				if (exts == null) {
					exts = new HashSet<String>();
					subject2ext.put(subject, exts);
				}
				exts.add(ext);
			}
			
			x++;
			
			if (x % 100000 == 0)
				System.out.println(x + " " + Util.memory());
		}
		
		is.close();
		ir.close();
		
		System.out.println("writing new index...");
		x = 0;
		IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(dir), true, new WhitespaceAnalyzer(), false);
		for (String object : object2ext.keySet()) {
			StringBuilder sb = new StringBuilder();
			for (String ext : object2ext.get(object))
				sb.append(ext).append("\n");
			
			Document doc = new Document();
			doc.add(new Field("oe", object, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
			doc.add(new Field("e", sb.toString(), Field.Store.YES, Field.Index.NO));
			
			iw.addDocument(doc);
			
			x++;
			
			if (x % 100000 == 0)
				System.out.println(x + "/" + object2ext.keySet().size() + " objects");
		}

		x = 0;
		for (String subject : subject2ext.keySet()) {
			StringBuilder sb = new StringBuilder();
			for (String ext : subject2ext.get(subject))
				sb.append(ext).append("\n");
			
			Document doc = new Document();
			doc.add(new Field("se", subject, Field.Store.NO, Field.Index.UN_TOKENIZED, Field.TermVector.NO));
			doc.add(new Field("e", sb.toString(), Field.Store.YES, Field.Index.NO));
			
			iw.addDocument(doc);

			x++;
			
			if (x % 100000 == 0)
				System.out.println(x + "/" + subject2ext.keySet().size() + " subjects");
		}

		System.out.println("optimizing...");
		iw.optimize();
		iw.close();
		System.out.println("done");
		
		updateIndex(dir);
	}
}
