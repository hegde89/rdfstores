package edu.unika.aifb.graphindex.util;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
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

import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.query.StructuredQueryParser;

public class QueryLoader {
	private StructuredQueryParser m_parser;
	private Map<String,String> m_namespaces;
	
	private static final Logger log = Logger.getLogger(QueryLoader.class);

	public QueryLoader() {
		m_namespaces = new HashMap<String,String>();
		m_parser = new StructuredQueryParser(m_namespaces);
	}
	
	private StructuredQuery createQuery(String queryName, String query, List<String> selectNodes) throws IOException {
		StructuredQuery q = m_parser.parseQuery(query);
		q.setName(queryName);

		for (String node : selectNodes)
			q.setAsSelect(node);

		return q;
	}
	
	public List<StructuredQuery> loadQueryFile(String filename) throws IOException {
		return loadQueryFile(filename, false);
	}
	
	public List<StructuredQuery> loadQueryFile(String filename, boolean addPrefix) throws IOException {
		List<StructuredQuery> queries = new ArrayList<StructuredQuery>();
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		
		String currentQueryName = null, currentQuery = "";
		Set<String> removeNodes = new HashSet<String>();
		List<String> selectNodes = new ArrayList<String>();

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
					StructuredQuery q = createQuery(currentQueryName, currentQuery, selectNodes);
					if (q != null) {
						queries.add(q);
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
			StructuredQuery q = createQuery(currentQueryName, currentQuery, selectNodes);
			if (q != null) {
				queries.add(q);
			}
		}
		
		return queries;
	}
}
