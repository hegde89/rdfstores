package edu.unika.aifb.keywordsearch.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;

public class KeywordQueryParser extends MultiFieldQueryParser {

	public KeywordQueryParser(String[] fields, Analyzer analyzer) {
		super(fields, analyzer);
	}
	
	public Query parse(String query, String[] fields) throws ParseException {
		BooleanQuery bQuery = new BooleanQuery();
		for (int i = 0; i < fields.length; i++) {
			QueryParser qp = new QueryParser(fields[i], getAnalyzer());
			qp.setDefaultOperator(QueryParser.AND_OPERATOR);
			Query q = qp.parse(query);
			bQuery.add(q, Occur.SHOULD);
		}
		return bQuery;
	}

	public Query parse(String query) throws ParseException {
		BooleanQuery bQuery = new BooleanQuery();
		for (int i = 0; i < fields.length; i++) {
			QueryParser qp = new QueryParser(fields[i], getAnalyzer());
			qp.setDefaultOperator(QueryParser.AND_OPERATOR);
			Query q = qp.parse(query);
			bQuery.add(q, Occur.SHOULD);
		}
		return bQuery;
	}
}
