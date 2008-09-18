package edu.unika.aifb.graphindex;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.Constant;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.Literal;
import edu.unika.aifb.graphindex.query.Predicate;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.Term;
import edu.unika.aifb.graphindex.query.Variable;

public class QueryParser {
	private static final Logger log = Logger.getLogger(QueryParser.class);

	public void QueryParser() {
		
	}
	
	public Query parseQuery(String queryString) throws IOException {
		Set<String> vars = new HashSet<String>();
		List<Literal> lits = new ArrayList<Literal>();
		
		String[] lines = queryString.split("\n");
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
						subject = new Individual(tokenizer.sval);
				}
				else if (i == 1) {
					property = new Predicate(tokenizer.sval);
				}
				else if (i == 2) {
					if (tokenizer.sval.startsWith("?")) {
						object = new Variable(tokenizer.sval);
						vars.add(tokenizer.sval);
					}
					else if (tokenizer.sval.startsWith("http"))
						object = new Individual(tokenizer.sval);
					else 
						object = new Constant(tokenizer.sval);
				}
				
				i++;
			}
			
//			log.debug(subject + " " + property + " " + object);
			lits.add(new Literal(property, subject, object));
		}
		
		Query q = new Query(vars.toArray(new String[] {}));
		for (Literal l : lits)
			q.addLiteral(l);
		
		return q;
	}
}
