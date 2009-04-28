package edu.unika.aifb.query.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.query.query.AtomQuery;
import edu.unika.aifb.query.query.PathQuery;
import edu.unika.aifb.query.query.SQuery;
import edu.unika.aifb.query.query.StarQuery;

public class PathQueryGenerator extends AbstractGenerator {
	
	public PathQueryGenerator(String indexDir, String outputFile, int maxAtom, int maxVar, int numQuery) throws StorageException, IOException {
		super(indexDir, outputFile, maxAtom, maxVar, numQuery);
	}
	
	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1, numAtom;
		boolean hasConstant = false;
//		numAtom = r.nextInt(maxAtom - 3) + 4;
		if(maxAtom <= 10)
			numAtom = 10;
		else
			numAtom = r.nextInt(maxAtom - 10) + 11;
		int degree = 2;
		Document entry = dataSearcher.doc(rn);
		PathQuery pq = new PathQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		pq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		pq.setVisitedVertices(visitedVertices);
		
		ArrayList<String> frontier = new ArrayList<String>(); 
		TermQuery qfw, qbw;
		Hits hitsfw, hitsbw;
		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
		hitsfw = dataSearcher.search(qfw);
		Iterator iterfw = hitsfw.iterator();
		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
		hitsbw = dataSearcher.search(qbw);
		Iterator iterbw = hitsbw.iterator();
		if((hitsfw.length() + hitsbw.length()) < degree)
			return null;
		while(i < degree) {
			boolean b;
			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
				b = r.nextBoolean();
			}
			else if (iterfw.hasNext() == true) {
				b = true;
			}
			else if (iterbw.hasNext() == true) {
				b = false; 
			}
			else return null;
			if(b == true) {
				Hit hit = (Hit)iterfw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				isCon = entry.get(Generator.FIELD_CONSTANT);
				if(isCon.equals("true")) continue;
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				AtomQuery atom = new AtomQuery(subject, predicate, object);
					
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						frontier.add(object);
						visitedVertices.add(object);
					}
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
			else {
				Hit hit = (Hit)iterbw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				AtomQuery atom = new AtomQuery(subject, predicate, object);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					visitedVertices.add(subject);
					frontier.add(subject);
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
		}
		
		while(i < numAtom && frontier.size() != 0) {
			int s = r.nextInt(frontier.size());
			String vertex = frontier.get(s);
			frontier.remove(s);
			qfw = new TermQuery(new Term(Generator.FIELD_SRC, vertex));
			hitsfw = dataSearcher.search(qfw);
			iterfw = hitsfw.iterator();
			qbw = new TermQuery(new Term(Generator.FIELD_DST, vertex));
			hitsbw = dataSearcher.search(qbw);
			iterbw = hitsbw.iterator();
			if((hitsfw.length() + hitsbw.length()) == 0) {
				continue;
			}
			boolean b;
			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
				b = r.nextBoolean() & r.nextBoolean();
			}
			else if (iterbw.hasNext() == true) {
				b = false;
			}
			else if (iterfw.hasNext() == true) {
				b = true; 
			}
			else return null;
			if(b == true) {
				while(iterfw.hasNext() == true) {
					Hit hit = (Hit)iterfw.next();
					entry = hit.getDocument();
					subject = entry.get(Generator.FIELD_SRC);
					predicate = entry.get(Generator.FIELD_EDGE);
					object = entry.get(Generator.FIELD_DST);
					isCon = entry.get(Generator.FIELD_CONSTANT);
					if(isCon.equals("false")) break;
					if(isCon.equals("true") && r.nextBoolean() || hasConstant == true) continue;
					break;
				}
				
				if(isCon.equals("true"))
					hasConstant = true; 
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				AtomQuery atom = new AtomQuery(subject, predicate, object);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						frontier.add(s, object);
						visitedVertices.add(object);
					}
					else {
						verticesWithLit.add(subject);
					}
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
			else {
				Hit hit = (Hit)iterbw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				isCon = entry.get(Generator.FIELD_CONSTANT);
				AtomQuery atom = new AtomQuery(subject, predicate, object);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					frontier.add(s, subject);
					visitedVertices.add(subject);
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
		}
		if(frontier.size() != 0) {
			pq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return pq;
		
	}

	public SQuery generateQueryWithoutResults(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1, numAtom;
		boolean hasConstant = false;
//		numAtom = r.nextInt(maxAtom - 3) + 4;
		if(maxAtom <= 10)
			numAtom = 10;
		else
			numAtom = r.nextInt(maxAtom - 10) + 11;
		int degree = 2;
		Document entry = dataSearcher.doc(rn);
		PathQuery pq = new PathQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		pq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		pq.setVisitedVertices(visitedVertices);
		
		ArrayList<String> frontier = new ArrayList<String>(); 
		TermQuery qfw, qbw;
		Hits hitsfw, hitsbw;
		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
		hitsfw = dataSearcher.search(qfw);
		Iterator iterfw = hitsfw.iterator();
		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
		hitsbw = dataSearcher.search(qbw);
		Iterator iterbw = hitsbw.iterator();
		if((hitsfw.length() + hitsbw.length()) < degree)
			return null;
		while(i < degree) {
			boolean b;
			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
				b = r.nextBoolean();
			}
			else if (iterfw.hasNext() == true) {
				b = true;
			}
			else if (iterbw.hasNext() == true) {
				b = false; 
			}
			else return null;
			if(b == true) {
				Hit hit = (Hit)iterfw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				isCon = entry.get(Generator.FIELD_CONSTANT);
				if(isCon.equals("true")) continue;
				AtomQuery atom = new AtomQuery(object, predicate, subject);
					
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						frontier.add(object);
						visitedVertices.add(object);
					}
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
			else {
				Hit hit = (Hit)iterbw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				AtomQuery atom = new AtomQuery(object, predicate, subject);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					visitedVertices.add(subject);
					frontier.add(subject);
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
		}
		
		while(i < numAtom && frontier.size() != 0) {
			int s = r.nextInt(frontier.size());
			String vertex = frontier.get(s);
			frontier.remove(s);
			qfw = new TermQuery(new Term(Generator.FIELD_SRC, vertex));
			hitsfw = dataSearcher.search(qfw);
			iterfw = hitsfw.iterator();
			qbw = new TermQuery(new Term(Generator.FIELD_DST, vertex));
			hitsbw = dataSearcher.search(qbw);
			iterbw = hitsbw.iterator();
			if((hitsfw.length() + hitsbw.length()) == 0) {
				continue;
			}
			boolean b;
			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
				b = r.nextBoolean() & r.nextBoolean();
			}
			else if (iterbw.hasNext() == true) {
				b = false;
			}
			else if (iterfw.hasNext() == true) {
				b = true; 
			}
			else return null;
			if(b == true) {
				while(iterfw.hasNext() == true) {
					Hit hit = (Hit)iterfw.next();
					entry = hit.getDocument();
					subject = entry.get(Generator.FIELD_SRC);
					predicate = entry.get(Generator.FIELD_EDGE);
					object = entry.get(Generator.FIELD_DST);
					isCon = entry.get(Generator.FIELD_CONSTANT);
					if(isCon.equals("false")) break;
					if(isCon.equals("true") && r.nextBoolean() || hasConstant == true) continue;
					break;
				}
				AtomQuery atom;
				if(isCon.equals("true"))
					hasConstant = true; 
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				if(isCon.equals("true"))
					atom = new AtomQuery(subject, predicate, object);
				else 
					atom = new AtomQuery(object, predicate, subject);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						frontier.add(s, object);
						visitedVertices.add(object);
					}
					else {
						verticesWithLit.add(subject);
					}
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
			else {
				Hit hit = (Hit)iterbw.next();
				entry = hit.getDocument();
				subject = entry.get(Generator.FIELD_SRC);
				predicate = entry.get(Generator.FIELD_EDGE);
				object = entry.get(Generator.FIELD_DST);
				isCon = entry.get(Generator.FIELD_CONSTANT);
				AtomQuery atom = new AtomQuery(object, predicate, subject);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					frontier.add(s, subject);
					visitedVertices.add(subject);
				}
				else continue;
				i++;
				pq.addAtom(atom);
			}
		}
		if(frontier.size() != 0) {
			pq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return pq;
	}

//	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
//		int rn = r.nextInt(maxDoc);
//		Document atom = graphSearcher.doc(rn);
//		int i = 1;
//		int j = 1;
//		String subject = atom.get(Generator.FIELD_SRC);
//		String predicate = atom.get(Generator.FIELD_EDGE);
//		String object = atom.get(Generator.FIELD_DST);
//		TermQuery q = new TermQuery(new Term(Generator.FIELD_BLOCK, object));
//		Hits hits = blockSearcher.search(q);
//		if (hits != null && hits.length() != 0) {
//			rn = r.nextInt(hits.length());
//			Document block = hits.doc(rn);
//			object = block.get(Generator.FIELD_ELE);
//		}	
//		Map<String, String> map = new HashMap<String, String>();
//		String var = "x" + j++;
//		map.put(subject, var);
//		PathQuery pq = new PathQuery();
//		pq.addVariables(var);
//		pq.addAtom(var, predicate, object.startsWith("http://") ? object : "\"" + object + "\"");
//		while(i < maxAtom ) {
//			q = new TermQuery(new Term(Generator.FIELD_DST, subject));
//			hits = graphSearcher.search(q);
//			if(hits == null || hits.length() == 0) return null;
//			rn = r.nextInt(hits.length()); 
//			atom = hits.doc(rn);
//			subject = atom.get(Generator.FIELD_SRC);
//			predicate = atom.get(Generator.FIELD_EDGE);
//			object = atom.get(Generator.FIELD_DST);
//			var = "x" + j++;
//			map.put(subject, var);
//			pq.addVariables(var);
//			pq.addAtom(var, predicate, map.get(object));
//			i++;
//		}
//		
//		return pq;
//	}

}
