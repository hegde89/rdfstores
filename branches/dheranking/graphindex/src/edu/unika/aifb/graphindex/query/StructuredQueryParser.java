package edu.unika.aifb.graphindex.query;

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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class StructuredQueryParser {
	private static final Logger log = Logger.getLogger(StructuredQueryParser.class);
	private Map<String,String> m_namespaces;

	public StructuredQueryParser() {
		m_namespaces = new HashMap<String,String>();
	}
	
	public StructuredQueryParser(Map<String,String> namespaces) {
		m_namespaces = namespaces;
	}
	
	public String resolveNamespace(String uri) {
		for (String ns : m_namespaces.keySet()) {
			if (uri.startsWith(ns + ":")) {
				return uri.replaceFirst(ns + ":", m_namespaces.get(ns));
			}
		}
		return uri;
	}
	
	public StructuredQuery parseQuery(String queryString) throws IOException {
		List<String> vars = new ArrayList<String>();
		
		StructuredQuery q = new StructuredQuery("");
		
		String[] lines = queryString.split("\n");

		for (String line : lines) {
			StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(line));
			tokenizer.wordChars('(', '~');
			tokenizer.wordChars('!', '!');
			tokenizer.wordChars('#', '&');
			
			String property = null, subject = null, object = null;
			
			int token;
			int i = 0;
			while ((token = tokenizer.nextToken()) != StreamTokenizer.TT_EOF) {
//				log.debug(token + " " + tokenizer.nval + " " + tokenizer.sval);
				
				if (i == 0) {
					if (tokenizer.sval.startsWith("?")) {
						subject = tokenizer.sval;
					}
					else
						subject = resolveNamespace(tokenizer.sval);
				}
				else if (i == 1) {
					property = resolveNamespace(tokenizer.sval);
				}
				else if (i == 2) {
					if (tokenizer.sval.startsWith("?")) {
						object = tokenizer.sval;
						if (!vars.contains(tokenizer.sval))
							vars.add(tokenizer.sval);
					}
					else if (tokenizer.sval.startsWith("http") || tokenizer.sval.contains(":"))
						object = resolveNamespace(tokenizer.sval);
					else 
						object = tokenizer.sval;
				}
				
				i++;
			}

			q.addEdge(subject, property, object);
		}
		
		
		return q;
	}
}
