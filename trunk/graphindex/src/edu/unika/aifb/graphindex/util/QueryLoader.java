package edu.unika.aifb.graphindex.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Query;

public class QueryLoader {
	private QueryParser m_parser;
	private Map<String,String> m_namespaces;

	public QueryLoader() {
		m_namespaces = new HashMap<String,String>();
		m_parser = new QueryParser(m_namespaces);
	}
	
	public List<Query> loadQueryFile(String filename) throws IOException {
		List<Query> queries = new ArrayList<Query>();
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		
		String currentQueryName = null, currentQuery = "";
		
		String input;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			
			if (input.startsWith("ns:")) {
				String[] t = input.split(" ");
				m_namespaces.put(t[1], t[2]);
				continue;
			}
			
			if (input.startsWith("query:")) {
				if (currentQuery.length() > 0) {
					Query q = m_parser.parseQuery(currentQuery);
					q.setName(currentQueryName);
					queries.add(q);
				}

				String[] t = input.split(":");
				currentQueryName = t[1].trim();
				
				currentQuery = "";
			}
			else {
				if (!input.equals(""))
					currentQuery += input + "\n";
			}
		}
		
		Query q = m_parser.parseQuery(currentQuery);
		q.setName(currentQueryName);
		queries.add(q);
		
		return queries;
	}
}
