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

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Query;

public class QueryLoader {
	private QueryParser m_parser;
	private Map<String,String> m_namespaces;
	private Set<String> m_bwEdgeSet;
	private Set<String> m_fwEdgeSet;
	
	private static final Logger log = Logger.getLogger(QueryLoader.class);

	public QueryLoader() {
		m_namespaces = new HashMap<String,String>();
		m_parser = new QueryParser(m_namespaces);
	}
	
	public Set<String> getForwardEdgeSet() {
		return m_fwEdgeSet;
	}
	
	public Set<String> getBackwardEdgeSet() {
		return m_bwEdgeSet;
	}
	
	private Query createQuery(String queryName, String query, List<String> selectNodes) throws IOException {
		Query q = m_parser.parseQuery(query);
		q.setName(queryName);
//		q.setRemoveNodes(removeNodes);
		q.setSelectVariables(selectNodes);
		q.createQueryGraph();
		
		m_fwEdgeSet.addAll(q.getForwardEdgeSet());
		m_bwEdgeSet.addAll(q.getBackwardEdgeSet());
		m_bwEdgeSet.addAll(q.getNeutralEdgeSet());
		
		log.debug("query: " + q.getName());
		log.debug("  ne: " + q.getNeutralEdgeSet());
		log.debug("  bw: " + q.getBackwardEdgeSet());
		log.debug("  fw: " + q.getForwardEdgeSet());
		
		return q;
	}
	
	public List<Query> loadQueryFile(String filename) throws IOException {
		List<Query> queries = new ArrayList<Query>();
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		
		String currentQueryName = null, currentQuery = "";
		Set<String> removeNodes = new HashSet<String>();
		List<String> selectNodes = new ArrayList<String>();
		m_fwEdgeSet = new HashSet<String>();
		m_bwEdgeSet = new HashSet<String>();
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
					queries.add(createQuery(currentQueryName, currentQuery, selectNodes));
				}

				String[] t = input.split(":");
				currentQueryName = t[1].trim();
				
				currentQuery = "";
				removeNodes = new HashSet<String>();
				selectNodes = new ArrayList<String>();
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
		
		queries.add(createQuery(currentQueryName, currentQuery, selectNodes));
		
		log.debug("bw edge set");
		for (String s : m_bwEdgeSet)
			System.out.println(s);
		log.debug("fw edge set");
		for (String s : m_fwEdgeSet)
			System.out.println(s);
		
		return queries;
	}
}
