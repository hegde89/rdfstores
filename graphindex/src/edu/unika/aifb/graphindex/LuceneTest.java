package edu.unika.aifb.graphindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import edu.unika.aifb.graphindex.storage.Triple;

public class LuceneTest {
	private static IndexReader m_reader;
	private static IndexSearcher m_searcher;
	
	private final static String FIELD_EXT = "ext";
	private final static String FIELD_SUBJECT = "subject";
	private final static String FIELD_PROPERTY = "property";
	private final static String FIELD_OBJECT = "object";
	
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
	
	public static void main(String[] args) throws CorruptIndexException, IOException {
		m_reader = IndexReader.open("/Users/gl/Studium/diplomarbeit/workspace/graphindex/output/sweto2/index");
		m_searcher = new IndexSearcher(m_reader);
		
		executeQuery(new TermQuery(new Term(FIELD_EXT, "b334672__http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name__Pattern Recognition__")));
		executeQuery(new TermQuery(new Term(FIELD_EXT, "b339936__http://lsdis.cs.uga.edu/projects/semdis/opus#journal_name__Pattern Recognition__")));
		executeQuery(new TermQuery(new Term(FIELD_EXT, "b338678__http://www.w3.org/1999/02/22-rdf-syntax-ns#type__http://lsdis.cs.uga.edu/projects/semdis/opus#Article__")));
		executeQuery(new PrefixQuery(new Term(FIELD_EXT, "b338678__")));
	}
}
