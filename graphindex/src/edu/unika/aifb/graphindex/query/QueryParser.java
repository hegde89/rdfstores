package edu.unika.aifb.graphindex.query;

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

import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;

public class QueryParser {
	private static final Logger log = Logger.getLogger(QueryParser.class);
	private Map<String,String> m_namespaces;

	public QueryParser() {
		m_namespaces = new HashMap<String,String>();
	}
	
	public QueryParser(Map<String,String> namespaces) {
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
	
	public Query parseQuery(String queryString) throws IOException {
		Set<String> vars = new HashSet<String>();
		List<Literal> lits = new ArrayList<Literal>();
		Map<String,Integer> e2s = new HashMap<String,Integer>();
		
		String[] lines = queryString.split("\n");
		int x = 0;
		for (String line : lines) {
			StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(line));
			tokenizer.wordChars('(', '~');
			tokenizer.wordChars('!', '!');
			tokenizer.wordChars('#', '&');
			
			Term subject = null, object = null;
			Predicate property = null;
			
			int token;
			int i = 0;
			while ((token = tokenizer.nextToken()) != StreamTokenizer.TT_EOF) {
//				log.debug(token + " " + tokenizer.nval + " " + tokenizer.sval);
				
				if (i == 0) {
					if (tokenizer.sval.startsWith("?")) {
						subject = new Variable(tokenizer.sval);
						vars.add(tokenizer.sval);
					}
					else
						subject = new Individual(resolveNamespace(tokenizer.sval));
				}
				else if (i == 1) {
					property = new Predicate(resolveNamespace(tokenizer.sval));
				}
				else if (i == 2) {
					if (tokenizer.sval.startsWith("?")) {
						object = new Variable(tokenizer.sval);
						vars.add(tokenizer.sval);
					}
					else if (tokenizer.sval.startsWith("http") || tokenizer.sval.contains(":"))
						object = new Individual(resolveNamespace(tokenizer.sval));
					else 
						object = new Constant(tokenizer.sval);
				}
				
				i++;
			}
			
//			log.debug(subject + " " + property + " " + object);
			lits.add(new Literal(property, subject, object));
			e2s.put(subject + " " + property + " " + object, x);
			x++;
		}
		
		Query q = new Query(vars);
		for (Literal l : lits)
			q.addLiteral(l);
		q.setEvalOrder(e2s);
		
		return q;
	}
}
