package edu.unika.aifb.querygenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.Document;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.query.AtomQuery;
import edu.unika.aifb.query.SQuery;
import edu.unika.aifb.query.generator.Generator;

public class AtomQueryGenerator extends AbstractGenerator {
	
	public AtomQueryGenerator(String indexDir, String outputFile, int maxAtom, int maxVar, int numQuery) throws StorageException, IOException {
		super(indexDir, outputFile, 1, 1, numQuery);
	}
	
	public AtomQueryGenerator(String indexDir, String outputFile, int numQuery) throws StorageException, IOException {
		super(indexDir, outputFile, 1, 1, numQuery);
	}

	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1;
		Document entry = dataSearcher.doc(rn);
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = entry.get(Generator.FIELD_EDGE);
		String object = entry.get(Generator.FIELD_DST);
		String isLit = entry.get(Generator.FIELD_CONSTANT);
		if(isLit.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			if(r.nextInt(3)!= 0) return null;
			object = "'" + object + "'";
		}
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		AtomQuery aq = new AtomQuery(subject, predicate, object);
		aq.setCenterElement(subject);
		aq.addAtom(aq);
		Set<String> visitedVertices = new HashSet<String>();
		ArrayList<String> frontier = new ArrayList<String>();
		visitedVertices.add(subject);
		if(!isLit.equals("true")) {
			visitedVertices.add(object);
			frontier.add(object);
		}	
		aq.setVisitedVertices(visitedVertices);
		if(frontier.size() != 0) {
			aq.setFrontierVertices(new HashSet<String>(frontier));
		}
		
		return aq;
	}

	public SQuery generateQueryWithoutResults(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1;
		Document entry = dataSearcher.doc(rn);
		AtomQuery aq;
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = entry.get(Generator.FIELD_EDGE);
		String object = entry.get(Generator.FIELD_DST);
		String isLit = entry.get(Generator.FIELD_CONSTANT);
		if(isLit.equals("true")) {
			if(r.nextInt(3)!= 0) return null;
			object = "'" + object + "_'";
			aq = new AtomQuery(subject, predicate, object);
		}
		else {
			predicate += "_";
			aq = new AtomQuery(subject, predicate, object);
		}
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		
		aq.setCenterElement(subject);
		aq.addAtom(aq);
		Set<String> visitedVertices = new HashSet<String>();
		ArrayList<String> frontier = new ArrayList<String>();
		visitedVertices.add(subject);
		if(!isLit.equals("true")) {
			visitedVertices.add(object);
			frontier.add(object);
		}	
		aq.setVisitedVertices(visitedVertices);
		if(frontier.size() != 0) {
			aq.setFrontierVertices(new HashSet<String>(frontier));
		}
		
		return aq;
		
//		int rn = r.nextInt(maxDoc);
//		Document atom = graphSearcher.doc(rn);
//		String var = "?x1";
//		AtomQuery aq;
//		String subject = atom.get(Generator.FIELD_SRC);
//		String predicate = atom.get(Generator.FIELD_EDGE);
//		String object = atom.get(Generator.FIELD_DST);
//		
//
//		TermQuery q = new TermQuery(new Term(Generator.FIELD_BLOCK, object));
//		Hits hits = blockSearcher.search(q);
//		if (hits != null && hits.length()!= 0) {
//			int length = hits.length();
//			int i = r.nextInt(length);
//			Document block = hits.doc(i);
//			String isLit = block.get(Generator.FIELD_LIT);
//			object = block.get(Generator.FIELD_ELE);
//			if(predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
//				aq = new AtomQuery(var, predicate, object + "_NoClass");
//				aq.addVariables(var);
//				return aq;
//			} 
//			else if(isLit.equals("true")) {
//				aq = new AtomQuery(var, predicate, "'" + object + "_NoLiteral" + "'");
//				aq.addVariables(var);
//				return aq;
//			} 
//			else {
//				aq = new AtomQuery(object, predicate, var);
//				aq.addVariables(var);
//				return aq;
//			}
//			
//		}
//		
//		return null;
	}
	
//	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
//		int rn = r.nextInt(maxDoc);
//		Document atom = graphSearcher.doc(rn);
//		int j = 1; 
//		String subject = atom.get(Generator.FIELD_SRC);
//		String predicate = atom.get(Generator.FIELD_EDGE);
//		String object = atom.get(Generator.FIELD_DST);
//
//		TermQuery q = new TermQuery(new Term(Generator.FIELD_BLOCK, object));
//		Hits hits = blockSearcher.search(q);
//		if (hits != null && hits.length()!= 0) {
//			int length = hits.length();
//			int i = r.nextInt(length);
//			Document block = hits.doc(i);
//			object = block.get(Generator.FIELD_ELE);
//			String var = "?x" + j;
//			AtomQuery aq = new AtomQuery(var, predicate, object.startsWith("http://") ? object : "\"" + object + "\"");
//			aq.addVariables(var);
//			return aq;
//		}
//		
//		return null;
//	}

	
}
