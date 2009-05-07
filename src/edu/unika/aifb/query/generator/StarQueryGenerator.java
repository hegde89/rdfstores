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
import edu.unika.aifb.query.query.EntityQuery;
import edu.unika.aifb.query.query.SQuery;
import edu.unika.aifb.query.query.StarQuery;

public class StarQueryGenerator extends AbstractGenerator {

	public StarQueryGenerator(String indexDir, String outputFile, int maxAtom, int maxVar, int querySize) throws StorageException, IOException {
		super(indexDir, outputFile, maxAtom, maxVar, querySize);
	}

	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1, numAtom;
		if(maxAtom <= 10)
			numAtom = 10;
		else
			numAtom = r.nextInt(maxAtom - 10) + 11;
//		int degree = Math.max(r.nextInt((int)Math.floor(maxAtom/2)), 3);
		int degree = r.nextInt(numAtom -3) + 3;
		Document entry = dataSearcher.doc(rn);
		StarQuery sq = new StarQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		sq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		sq.setVisitedVertices(visitedVertices);
		
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
				sq.addAtom(atom);
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
				sq.addAtom(atom);
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
				if(isCon.equals("true") && r.nextBoolean()) continue;
//				while(iterfw.hasNext() == true) {
//					Hit hit = (Hit)iterfw.next();
//					entry = hit.getDocument();
//					subject = entry.get(Generator.FIELD_SRC);
//					predicate = entry.get(Generator.FIELD_EDGE);
//					object = entry.get(Generator.FIELD_DST);
//					isCon = entry.get(Generator.FIELD_CONSTANT);
//					if(isCon.equals("true") && r.nextBoolean()) continue;
//				}
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
				sq.addAtom(atom);
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
				sq.addAtom(atom);
			}
		}
		if(frontier.size() != 0) {
			sq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return sq;
		
	}

	public SQuery generateQueryWithoutResults(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1, numAtom;
		if(maxAtom <= 10)
			numAtom = 10;
		else
			numAtom = r.nextInt(maxAtom - 10) + 11;
//		int degree = Math.max(r.nextInt((int)Math.floor(maxAtom/2)), 3);
		int degree = r.nextInt(numAtom -3) + 3;
		Document entry = dataSearcher.doc(rn);
		StarQuery sq = new StarQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		sq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		sq.setVisitedVertices(visitedVertices);
		
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
				sq.addAtom(atom);
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
				sq.addAtom(atom);
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
				if(isCon.equals("true") && r.nextBoolean()) continue;
//				while(iterfw.hasNext() == true) {
//					Hit hit = (Hit)iterfw.next();
//					entry = hit.getDocument();
//					subject = entry.get(Generator.FIELD_SRC);
//					predicate = entry.get(Generator.FIELD_EDGE);
//					object = entry.get(Generator.FIELD_DST);
//					isCon = entry.get(Generator.FIELD_CONSTANT);
//					if(isCon.equals("true") && r.nextBoolean()) continue;
//				}
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				AtomQuery atom;
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
				sq.addAtom(atom);
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
				sq.addAtom(atom);
			}
		}
		if(frontier.size() != 0) {
			sq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return sq;
	}
	
//	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
//		int rn = r.nextInt(maxDoc);
//		int i = 1, j = 1;
////		int degree = Math.max(r.nextInt((int)Math.floor(maxAtom/2)), 3);
//		int degree = r.nextInt(maxAtom -3) + 3;
//		Document atom = graphSearcher.doc(rn);
//		StarQuery sq = new StarQuery();
//		String subject = atom.get(Generator.FIELD_SRC);
//		String predicate = atom.get(Generator.FIELD_EDGE);
//		String object = atom.get(Generator.FIELD_DST);
//		Set<AtomQuery> visitedAtom = new HashSet<AtomQuery>();
//		Set<String> visitedVertex = new HashSet<String>();
//		Set<Set<String>> visitedVertexPair = new HashSet<Set<String>>();
//		visitedAtom.add(new AtomQuery(subject, predicate, object));
//		visitedVertex.add(subject);
//		visitedVertex.add(object);
//		HashSet<String> pair = new HashSet<String>();
//		pair.add(subject);
//		pair.add(object);
//		visitedVertexPair.add(pair);
//		TermQuery q = new TermQuery(new Term(Generator.FIELD_BLOCK, object));
//		Hits hits = blockSearcher.search(q);
//		if (hits != null && hits.length() != 0) {
//			rn = r.nextInt(hits.length());
//			Document block = hits.doc(rn);
//			object = block.get(Generator.FIELD_ELE);
//		}
//		else 
//			return null;
//		Map<String, String> map = new HashMap<String, String>();
//		String var = "x" + j++;
//		map.put(subject, var);
//		sq.addVariables(var);
//		sq.addAtom(new AtomQuery(var, predicate, object.startsWith("http://") ? object : "\"" + object + "\""));
//		
//		ArrayList<String> frontier = new ArrayList<String>(); 
//		TermQuery qfw, qbw;
//		Hits hitsfw, hitsbw;
//		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
//		hitsfw = graphSearcher.search(qfw);
//		Iterator iterfw = hitsfw.iterator();
//		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
//		hitsbw = graphSearcher.search(qbw);
//		Iterator iterbw = hitsbw.iterator();
//		if((hitsfw.length() + hitsbw.length()) < degree)
//			return null;
//		while(i < degree) {
//			boolean b;
//			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
//				b = r.nextBoolean();
//			}
//			else if (iterfw.hasNext() == true) {
//				b = true;
//			}
//			else if (iterbw.hasNext() == true) {
//				b = false; 
//			}
//			else return null;
//			if(b == true) {
//				Hit hit = (Hit)iterfw.next();
//				atom = hit.getDocument();
//				subject = atom.get(Generator.FIELD_SRC);
//				predicate = atom.get(Generator.FIELD_EDGE);
//				object = atom.get(Generator.FIELD_DST);
//				AtomQuery str = new AtomQuery(subject, predicate, object);
//				if(!visitedAtom.contains(str)) {
//					visitedAtom.add(str);
//					visitedVertex.add(object);
//					pair = new HashSet<String>();
//					pair.add(subject);
//					pair.add(object);
//					visitedVertexPair.add(pair);
//				}
//				else continue;
//				i++;
//				var = "x" + j++;
//				map.put(object, var);
//				sq.addVariables(var);
//				sq.addAtom(new AtomQuery(map.get(subject), predicate, var));
//				frontier.add(object);
//			}
//			else {
//				Hit hit = (Hit)iterbw.next();
//				atom = hit.getDocument();
//				subject = atom.get(Generator.FIELD_SRC);
//				predicate = atom.get(Generator.FIELD_EDGE);
//				object = atom.get(Generator.FIELD_DST);
//				AtomQuery str = new AtomQuery(subject, predicate, object);
//				if(!visitedAtom.contains(str)) {
//					visitedAtom.add(str);
//					visitedVertex.add(subject);
//					pair = new HashSet<String>();
//					pair.add(subject);
//					pair.add(object);
//					visitedVertexPair.add(pair);
//				}
//				else continue;
//				i++;
//				var = "x" + j++;
//				map.put(subject, var);
//				sq.addVariables(var);
//				sq.addAtom(new AtomQuery(var, predicate, map.get(object)));
//				frontier.add(subject);
//			}
//		}
//		
//		while(i < maxAtom && frontier.size() != 0) {
//			int s = r.nextInt(frontier.size());
//			String vertex = frontier.get(s);
//			frontier.remove(s);
//			qfw = new TermQuery(new Term(Generator.FIELD_SRC, vertex));
//			hitsfw = graphSearcher.search(qfw);
//			iterfw = hitsfw.iterator();
//			qbw = new TermQuery(new Term(Generator.FIELD_DST, vertex));
//			hitsbw = graphSearcher.search(qbw);
//			iterbw = hitsbw.iterator();
//			if((hitsfw.length() + hitsbw.length()) == 0) {
//				continue;
//			}
//			boolean b;
//			if(iterfw.hasNext() == true && iterbw.hasNext() == true){
//				b = r.nextBoolean();
//			}
//			else if (iterfw.hasNext() == true) {
//				b = true;
//			}
//			else if (iterbw.hasNext() == true) {
//				b = false; 
//			}
//			else return null;
//			if(b == true) {
//				Hit hit = (Hit)iterfw.next();
//				atom = hit.getDocument();
//				subject = atom.get(Generator.FIELD_SRC);
//				predicate = atom.get(Generator.FIELD_EDGE);
//				object = atom.get(Generator.FIELD_DST);
//				pair = new HashSet<String>();
//				pair.add(subject);
//				pair.add(object);
//				if(!visitedVertex.contains(object) && !visitedVertexPair.contains(pair)) {
//					visitedVertex.add(object);
//				}
//				else continue;
//				visitedVertexPair.add(pair);
//				i++;
//				var = "x" + j++;
//				map.put(object, var);
//				sq.addVariables(var);
//				sq.addAtom(new AtomQuery(map.get(subject), predicate, var));
//				frontier.add(s, object);
//			}
//			else {
//				Hit hit = (Hit)iterbw.next();
//				atom = hit.getDocument();
//				subject = atom.get(Generator.FIELD_SRC);
//				predicate = atom.get(Generator.FIELD_EDGE);
//				object = atom.get(Generator.FIELD_DST);
//				pair = new HashSet<String>();
//				pair.add(subject);
//				pair.add(object);
//				if(!visitedVertex.contains(subject) && !visitedVertexPair.contains(pair)) {
//					visitedVertex.add(subject);
//				}
//				else continue;
//				visitedVertexPair.add(pair);
//				i++;
//				var = "x" + j++;
//				map.put(subject, var);
//				sq.addVariables(var);
//				sq.addAtom(new AtomQuery(var, predicate, map.get(object)));
//				frontier.add(s, subject);
//			}
//		}
//		
//		return sq;
//		
//	}
}
