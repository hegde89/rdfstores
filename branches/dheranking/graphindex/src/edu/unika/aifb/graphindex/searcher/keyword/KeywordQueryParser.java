package edu.unika.aifb.graphindex.searcher.keyword;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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
