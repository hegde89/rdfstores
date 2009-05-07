package edu.unika.aifb.graphindex;

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class LuceneRunner {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws CorruptIndexException 
	 */
	public static void main(String[] args) throws CorruptIndexException, IOException {
		IndexReader ir = IndexReader.open(args[0]);
		IndexSearcher is = new IndexSearcher(ir);
		
//		QueryParser qp = new QueryParser("oe", new WhitespaceAnalyzer());
//
//		Query q = qp.parse(args[2]);

		PrefixQuery pq = new PrefixQuery(new Term("oe", args[1]));
		System.out.println(pq);
		System.out.println();
		BooleanQuery bq = (BooleanQuery)pq.rewrite(ir);
		for (BooleanClause bc : bq.getClauses()) {
			TermQuery tq = (TermQuery)bc.getQuery();
			System.out.println(tq.getTerm().text() + " " + tq);
		}
	}

}
