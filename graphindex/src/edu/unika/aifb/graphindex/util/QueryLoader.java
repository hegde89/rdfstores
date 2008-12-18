package edu.unika.aifb.graphindex.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		Set<String> removeNodes = new HashSet<String>();
		Set<String> selectNodes = new HashSet<String>();
		String input;
		while ((input = in.readLine()) != null) {
			input = input.trim();
		
			if (input.startsWith("#"))
				continue;
		
			if (input.startsWith("ns:")) {
				String[] t = input.split(" ");
				m_namespaces.put(t[1], t[2]);
				continue;
			}
			
			if (input.startsWith("query:")) {
				if (currentQuery.length() > 0) {
					Query q = m_parser.parseQuery(currentQuery);
					q.setName(currentQueryName);
					q.setRemoveNodes(removeNodes);
					q.setSelectVariables(selectNodes);
					queries.add(q);
				}

				String[] t = input.split(":");
				currentQueryName = t[1].trim();
				
				currentQuery = "";
				removeNodes = new HashSet<String>();
				selectNodes = new HashSet<String>();
			}
			else {
				if (input.startsWith("select:")) {
					String[] t = input.split(" ");
					for (int i = 1; i < t.length; i++)
						selectNodes.add(t[i]);
				}
				else if (input.startsWith("remove:")) {
					String[] t = input.split(" ");
					for (int i = 1; i < t.length; i++)
						removeNodes.add(t[i]);
				}
				else if (!input.equals(""))
					currentQuery += input + "\n";
			}
			
		}
		
		Query q = m_parser.parseQuery(currentQuery);
		q.setName(currentQueryName);
		q.setRemoveNodes(removeNodes);
		q.setSelectVariables(selectNodes);
		queries.add(q);
		
		return queries;
	}
}
