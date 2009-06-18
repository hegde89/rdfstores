package edu.unika.aifb.graphindex.util;

import java.io.BufferedReader;
import java.io.File;
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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Query;

public class QueryLoader {
	private QueryParser m_parser;
	private StructureIndex m_index;
	private Map<String,String> m_namespaces;
	private Set<String> m_bwEdgeSet;
	private Set<String> m_fwEdgeSet;
	private Set<String> m_requiredBwEdgeSet;
	private Set<String> m_requiredFwEdgeSet;
	
	private static final Logger log = Logger.getLogger(QueryLoader.class);

	public QueryLoader() {
		this(null);
	}
	public QueryLoader(StructureIndex index) {
		m_index = index;
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

		// ignore the current edge sets, i.e. calculate prunable 
		// node as if all edges were in both sets
		q.setIgnoreIndexEdgeSets(true);
		q.createQueryGraph(m_index);
		// add edges to the requested edge set
		m_requiredFwEdgeSet.addAll(q.getForwardEdgeSet());
		m_requiredBwEdgeSet.addAll(q.getBackwardEdgeSet());
		m_requiredBwEdgeSet.addAll(q.getNeutralEdgeSet());

		q.setIgnoreIndexEdgeSets(false);
		q.createQueryGraph(m_index);
		
		if (q.getGraph() == null) {
			log.debug("query " + q.getName() + " not compatible with index edge set");
			return null;
		}
		
		
		m_fwEdgeSet.addAll(q.getForwardEdgeSet());
		m_bwEdgeSet.addAll(q.getBackwardEdgeSet());
		m_bwEdgeSet.addAll(q.getNeutralEdgeSet());
		
//		log.debug("query: " + q.getName());
//		log.debug("  ne: " + q.getNeutralEdgeSet());
//		log.debug("  bw: " + q.getBackwardEdgeSet());
//		log.debug("  fw: " + q.getForwardEdgeSet());
		
		return q;
	}
	
	public List<Query> loadQueryFile(String filename) throws IOException {
		return loadQueryFile(filename, false);
	}
	
	public List<Query> loadQueryFile(String filename, boolean addPrefix) throws IOException {
		List<Query> queries = new ArrayList<Query>();
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		
		String currentQueryName = null, currentQuery = "";
		Set<String> removeNodes = new HashSet<String>();
		List<String> selectNodes = new ArrayList<String>();
		m_fwEdgeSet = new HashSet<String>();
		m_bwEdgeSet = new HashSet<String>();
		m_requiredFwEdgeSet = new HashSet<String>();
		m_requiredBwEdgeSet = new HashSet<String>();
		int prunableQueries = 0;
		String input;
		String prefix = new File(filename).getName();
		boolean inQuery = false;
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
					Query q = createQuery(currentQueryName, currentQuery, selectNodes);
					if (q != null) {
						queries.add(q);
						if (q.getRemovedNodes().size() > 0)
							prunableQueries++;
					}
				}

				String[] t = input.split(":");
				currentQueryName = t[1].trim();
				if (addPrefix)
					currentQueryName = prefix + "_" + currentQueryName; 
				
				currentQuery = "";
				removeNodes = new HashSet<String>();
				selectNodes = new ArrayList<String>();
				inQuery = true;
			}
			else if (inQuery) {
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
				else
					inQuery = false;
			}
			
		}
		
		if (currentQuery != null && !currentQuery.equals("")) {
			Query q = createQuery(currentQueryName, currentQuery, selectNodes);
			if (q != null) {
				queries.add(q);
				if (q.getRemovedNodes().size() > 0)
					prunableQueries++;
			}
		}
		
		// the "requested" edge sets are the minimal sets to support maximal pruning 
		// for all queries in the query file
//		log.debug("req bw edge set " + m_requiredBwEdgeSet.size());
//		for (String s : m_requiredBwEdgeSet)
//			System.out.println(s);
//		log.debug("req fw edge set " + m_requiredFwEdgeSet.size());
//		for (String s : m_requiredFwEdgeSet)
//			System.out.println(s);
		log.debug("prunable: " + prunableQueries + "/" + queries.size());
		return queries;
	}
}
