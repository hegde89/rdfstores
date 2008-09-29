package edu.unika.aifb.graphindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.data.Triple;

public class LuceneTest {
	private static IndexWriter m_writer;
	private static IndexReader m_reader;
	private static IndexSearcher m_searcher;
	
	private final static String FIELD_EXT = "ext";
	private final static String FIELD_SUBJECT = "subject";
	private final static String FIELD_PROPERTY = "property";
	private final static String FIELD_OBJECT = "object";
	private final static String EXT_PATH_SEP = "__";
	
	public static void executeQuery(Query q) throws CorruptIndexException, IOException {
		long start = System.currentTimeMillis();
		System.out.println(q);
		final List<Integer> docIds = new ArrayList<Integer>();
		m_searcher.search(q, new HitCollector() {
			@Override
			public void collect(int docId, float score) {
				docIds.add(docId);
			}
		});
		long search = System.currentTimeMillis() - start;
		
//		Set<Triple> triples = new HashSet<Triple>(docIds.size());
//		List<Triple> tripleList = new ArrayList<Triple>(docIds.size() + 1);
		List<Triple> tripleList = new LinkedList<Triple>();
		Collections.sort(docIds);
		for (int docId : docIds) {
			Document doc = m_reader.document(docId, new FieldSelector() {
				@Override
				public FieldSelectorResult accept(String fieldName) {
					if (fieldName.equals(FIELD_SUBJECT))
						return FieldSelectorResult.LOAD;
					else
						return FieldSelectorResult.LOAD;
				}
			});
//			Document doc = m_reader.document(docId);
//			Triple t = new Triple(doc.getField(FIELD_SUBJECT).stringValue(), doc.getField(FIELD_PROPERTY).stringValue(), doc.getField(FIELD_OBJECT).stringValue());
			Triple t = new Triple(doc.getField(FIELD_SUBJECT).stringValue(), "", "");
//			triples.add(t);
			tripleList.add(t);
		}
		
		long retrieval = System.currentTimeMillis() - start - search;

		System.out.println("query: " + q + " (" + docIds.size() + " results) {" + (System.currentTimeMillis() - start) + " ms, s: " + search + " ms, r: " + retrieval + " ms}");
	}
	
	public static List<Integer> getDocIds(Query q) throws IOException {
		final List<Integer> docIds = new ArrayList<Integer>();
		m_searcher.search(q, new HitCollector() {
			@Override
			public void collect(int docId, float score) {
				docIds.add(docId);
			}
		});
		Collections.sort(docIds);
		return docIds;
	}
	
	public static void main(String[] args) throws CorruptIndexException, IOException {
//		String dir = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/sweto2/index";
		String dir = "/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/s/index";
		
		m_writer = new IndexWriter(FSDirectory.getDirectory(dir), true, new WhitespaceAnalyzer(), false);
		m_reader = IndexReader.open(dir);
		m_searcher = new IndexSearcher(m_reader);
		
		BooleanQuery.setMaxClauseCount(2000000);
		
//		executeQuery(new TermQuery(new Term(FIELD_EXT, "b334672__http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name__Pattern Recognition__")));
//		executeQuery(new TermQuery(new Term(FIELD_EXT, "b339936__http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name__Pattern Recognition__")));
//		executeQuery(new TermQuery(new Term(FIELD_EXT, "b338678__http://www.w3.org/1999/02/22-rdf-syntax-ns#type__http://lsdis.cs.uga.edu/projects/semdis/opus#Article__")));
//		executeQuery(new PrefixQuery(new Term(FIELD_EXT, "b338678__")));
		
		Set<String> uris = new HashSet<String>();
		List<Term> terms = new ArrayList<Term>();
		TermEnum te = m_reader.terms();
		while (te.next()) {
			Term t = te.term();
			if (t.field().equals(FIELD_EXT)) {
				terms.add(t);
				String[] path = t.text().split(EXT_PATH_SEP.replaceAll("\\|", "\\\\|"));
				uris.add(path[0]);
			}
		}
		
		System.out.println(uris.size());
		
		PrefixQuery pq = new PrefixQuery(new Term(FIELD_EXT, "b18"));
		BooleanQuery bq = (BooleanQuery)pq.rewrite(m_reader);
		for (BooleanClause bc : bq.getClauses())
			System.out.println(((TermQuery)bc.getQuery()).getTerm());
		
		for (Term t : terms) {
			List<Integer> docIds = getDocIds(new TermQuery(t));
			if (docIds.size() > 100)
				System.out.println(t + ": " + docIds.size() + " -> 1");
			if (docIds.size() > 1) {
				Set<String> subjects = new HashSet<String>();
				for (int docId : docIds) {
					Document doc = m_reader.document(docId, new FieldSelector() {
						@Override
						public FieldSelectorResult accept(String fieldName) {
							if (fieldName.equals(FIELD_SUBJECT))
								return FieldSelectorResult.LOAD;
							else
								return FieldSelectorResult.NO_LOAD;
						}
					});
					subjects.add(doc.getField(FIELD_SUBJECT).stringValue());
				}
				StringBuilder sb = new StringBuilder();
				for (String s : subjects)
					sb.append(s).append("");
				System.out.println(sb.toString());
			}
		}
		m_writer.close();
		m_searcher.close();
		m_reader.close();
//		int before = 0, after = 0;
//		int exts = 0;
//		for (String ext : uris) {
//			List<Integer> docIds = getDocIds(new PrefixQuery(new Term(FIELD_EXT, ext)));
//			Map<String,Set<String>> docMap = new HashMap<String,Set<String>>();
//			for (int docId : docIds) {
//				Document doc = m_reader.document(docId);
//				String idx = doc.getField(FIELD_OBJECT).stringValue() + "\t" + doc.getField(FIELD_PROPERTY).stringValue();
//				Set<String> subjects = docMap.get(idx);
//				if (subjects == null) {
//					subjects = new HashSet<String>();
//					docMap.put(idx, subjects);
//				}
//				subjects.add(doc.getField(FIELD_SUBJECT).stringValue());
//			}
//			before += docIds.size();
//			after += docMap.size();
//			System.out.println(ext + ": " + docIds.size() + " -> " + docMap.size());
//			exts++;
//			if (exts % 2000 == 0)
//				System.out.println(exts);
//		}
//		System.out.println(before + " -> " + after);
	}
}
