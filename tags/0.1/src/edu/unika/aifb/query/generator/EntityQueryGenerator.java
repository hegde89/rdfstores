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

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.query.query.AtomQuery;
import edu.unika.aifb.query.query.EntityQuery;
import edu.unika.aifb.query.query.SQuery;
import edu.unika.aifb.query.query.StarQuery;

public class EntityQueryGenerator extends AbstractGenerator {

	public EntityQueryGenerator(String indexDir, String outputFile, int maxAtom, int maxVar, int querySize) throws StorageException, IOException {
		super(indexDir, outputFile, maxAtom, 1, querySize);
	}

	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1;
		int numAtom = r.nextInt(maxAtom - 3) + 4;
		Document entry = dataSearcher.doc(rn);
		EntityQuery eq = new EntityQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		eq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		eq.setVisitedVertices(visitedVertices);
		
		ArrayList<String> frontier = new ArrayList<String>(); 
		TermQuery qfw, qbw;
		Hits hitsfw, hitsbw;
		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
		hitsfw = dataSearcher.search(qfw);
		Iterator iterfw = hitsfw.iterator();
		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
		hitsbw = dataSearcher.search(qbw);
		Iterator iterbw = hitsbw.iterator();
		if((hitsfw.length() + hitsbw.length()) < numAtom)
			return null;
		while(i < numAtom) {
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
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				AtomQuery atom = new AtomQuery(subject, predicate, object);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						visitedVertices.add(object);
						if(r.nextBoolean())
							frontier.add(object);
					}
				}
				else continue;
				i++;
				eq.addAtom(atom);
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
					if(r.nextBoolean())
						frontier.add(subject);
				}
				else continue;
				i++;
				eq.addAtom(atom);
			}
		}
		
		if(frontier.size() != 0) {
			eq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return eq;
	}

	public SQuery generateQueryWithoutResults(int maxDoc, Random r) throws IOException {
		int rn = r.nextInt(maxDoc);
		int i = 0, j = 1;
		int numAtom = r.nextInt(maxAtom - 3) + 4;
		Document entry = dataSearcher.doc(rn);
		EntityQuery eq = new EntityQuery();
		String subject = entry.get(Generator.FIELD_SRC);
		String predicate = null;
		String object = null;
		String isCon = null;
		if(centerElements.contains(subject)) return null;
		centerElements.add(subject);
		eq.setCenterElement(subject);
		
		Set<AtomQuery> visitedAtoms = new HashSet<AtomQuery>();
		Set<String> visitedVertices = new HashSet<String>();
		Set<String> verticesWithLit = new HashSet<String>();
		visitedVertices.add(subject);
		eq.setVisitedVertices(visitedVertices);
		
		ArrayList<String> frontier = new ArrayList<String>(); 
		TermQuery qfw, qbw;
		Hits hitsfw, hitsbw;
		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
		hitsfw = dataSearcher.search(qfw);
		Iterator iterfw = hitsfw.iterator();
		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
		hitsbw = dataSearcher.search(qbw);
		Iterator iterbw = hitsbw.iterator();
		if((hitsfw.length() + hitsbw.length()) < numAtom)
			return null;
		while(i < numAtom) {
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
				if(isCon.equals("true") && !predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					object = "'" + object + "'";
				}
				AtomQuery atom;
				if(isCon.equals("true"))
					atom= new AtomQuery(subject, predicate, object);
				else 
					atom= new AtomQuery(object, predicate, subject);
				if(!visitedAtoms.contains(atom)) {
					visitedAtoms.add(atom);
					if(!isCon.equals("true")) {
						visitedVertices.add(object);
						if(r.nextBoolean())
							frontier.add(object);
					}
				}
				else continue;
				i++;
				eq.addAtom(atom);
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
					if(r.nextBoolean())
						frontier.add(subject);
				}
				else continue;
				i++;
				eq.addAtom(atom);
			}
		}
		
		if(frontier.size() != 0) {
			eq.setFrontierVertices(new HashSet<String>(frontier));
		}
		return eq;
	}
	
//	public SQuery generateQuery(int maxDoc, Random r) throws IOException {
//		int rn = r.nextInt(maxDoc);
//		Document atom = graphSearcher.doc(rn);
//		EntityQuery eq = new EntityQuery();
//		int i = 1, j = 1;
//		String subject = atom.get(Generator.FIELD_SRC);
//		String predicate = atom.get(Generator.FIELD_EDGE);
//		String object = atom.get(Generator.FIELD_DST);
//		Set<AtomQuery> visited = new HashSet<AtomQuery>();
//		visited.add(new AtomQuery(subject, predicate, object));
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
//		eq.addVariables(var);
//		eq.addAtom(new AtomQuery(var, predicate, object.startsWith("http://") ? object : "\"" + object + "\""));
//		TermQuery qfw, qbw;
//		Hits hitsfw, hitsbw;
//		qfw = new TermQuery(new Term(Generator.FIELD_SRC, subject));
//		hitsfw = graphSearcher.search(qfw);
//		Iterator iterfw = hitsfw.iterator();
//		qbw = new TermQuery(new Term(Generator.FIELD_DST, subject));
//		hitsbw = graphSearcher.search(qbw);
//		Iterator iterbw = hitsbw.iterator();
//		if((hitsfw.length() + hitsbw.length()) < maxAtom)
//			return null;
//		while(i < maxAtom) {
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
//				if(!visited.contains(str)) {
//					visited.add(str);
//				}
//				else continue;
//				i++;
//				var = "x" + j++;
//				map.put(object, var);
//				eq.addVariables(var);
//				eq.addAtom(new AtomQuery(map.get(subject), predicate, var));
//			}
//			else {
//				Hit hit = (Hit)iterbw.next();
//				atom = hit.getDocument();
//				subject = atom.get(Generator.FIELD_SRC);
//				predicate = atom.get(Generator.FIELD_EDGE);
//				object = atom.get(Generator.FIELD_DST);
//				AtomQuery str = new AtomQuery(subject, predicate, object);
//				if(!visited.contains(str)) {
//					visited.add(str);
//				}
//				else continue;
//				i++;
//				var = "x" + j++;
//				map.put(subject, var);
//				eq.addVariables(var);
//				eq.addAtom(new AtomQuery(var, predicate, map.get(object)));
//			}
//		}
//		
//		return eq;
//	}
}
