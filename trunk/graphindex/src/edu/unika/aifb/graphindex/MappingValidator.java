package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.query.Constant;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.query.Term;
import edu.unika.aifb.graphindex.query.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class MappingValidator implements Callable<Set<Map<String,String>>> {

	private NamedQueryGraph<String,LabeledQueryEdge<String>> m_queryGraph;
	private IsomorphismRelation<String,LabeledEdge<String>> m_iso;
	private Map<String,Boolean> m_groundTermCache;
	private ExtensionManager m_em;
	private static final Logger log = Logger.getLogger(MappingValidator.class);
	
	public MappingValidator(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso, Map<String,Boolean> groundTermCache) {
		m_iso = iso;
		m_queryGraph = queryGraph;
		m_groundTermCache = groundTermCache;
		m_em = StorageManager.getInstance().getExtensionManager();
	}
	
	private Set<Map<String,String>> createMappings(String prefix, NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, String qv, Map<String,String> mapping, Triple triple) {
		Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
		
//		log.debug(prefix + "qv: " + qv + ", t.object: " + triple.getObject() + " " + mapping);
		
		for (LabeledEdge<String> outEdge : queryGraph.outgoingEdgesOf(qv)) {
			String property = outEdge.getLabel();
			String nv = outEdge.getDst();
			Set<Map<String,String>> localMappings = new HashSet<Map<String,String>>();
			
			if (!mapping.containsKey(nv)) {
				Map<String,String> newMap = new HashMap<String,String>(mapping);
				newMap.put(qv, triple.getObject());
				for (Triple nt : triple.getNext()) {
					if (nt.getProperty().equals(property)) {
//						log.debug(prefix + "nt: " + nt);
						localMappings.addAll(createMappings(prefix + "\t", queryGraph, nv, new HashMap<String,String>(newMap), nt));
					}
				}
			}
//			log.debug(prefix + "lm: " + localMappings);
			
			if (mappings.size() == 0)
				mappings.addAll(localMappings);
			else {
				for (Map<String,String> map : mappings) {
					for (Map<String,String> lm : localMappings) {
						boolean verified = true;
						for (String v : map.keySet()) {
							if (lm.containsKey(v) && !lm.get(v).equals(map.get(v))) {
								verified = false;
								break;
							}
						}
						if (verified)
							map.putAll(lm);
					}
				}
			}
		}
		
		for (LabeledEdge<String> inEdge : queryGraph.incomingEdgesOf(qv)) {
			break;
		}
		
		if (mappings.size() == 0) {
			mapping.put(qv, triple.getObject());
			mappings.add(mapping);
		}
//		log.debug(prefix + mappings);
		return mappings;
	}

	/**
	 * target.subject = source.object
	 * 
	 * @param target
	 * @param leftProperty
	 * @param source
	 * @return
	 * @throws StorageException
	 */
	public List<TriplePair> join(TripleSet targetTriples, String leftProperty, TripleSet sourceTriples) throws StorageException {
//		start(JOIN);
		// TODO use smaller triple set to create hash table (can't just swap, because we use
		// different columns on each side)
		
		Map<String,List<Triple>> targetSubject2Triples = new HashMap<String,List<Triple>>();
		for (Triple targetTriple : targetTriples.getTriples()) {
			List<Triple> triples = targetSubject2Triples.get(targetTriple.getSubject());
			if (triples == null) {
				triples = new ArrayList<Triple>();
				targetSubject2Triples.put(targetTriple.getSubject(), triples);
			}
			triples.add(targetTriple);
		}
		
		List<TriplePair> result = new LinkedList<TriplePair>();
		
		for (Triple sourceTriple : sourceTriples.getTriples()) {
			List<Triple> tts = targetSubject2Triples.get(sourceTriple.getObject());
			if (tts != null && tts.size() > 0) {
				for (Triple tt : tts) {
					result.add(new TriplePair(tt, sourceTriple));
					tt.addPrev(sourceTriple);
					sourceTriple.addNext(tt);
				}
			}
		}
		
//		end(JOIN);
		return result;
	}
	
	private Set<Map<String,String>> validateMapping2(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso) throws StorageException {
		
		List<LabeledEdge<String>> edges = new ArrayList<LabeledEdge<String>>();
		
		Map<String,TripleSet> tripleMapping = new HashMap<String,TripleSet>();
		
		for (String v : queryGraph.getGroundTerms()) {
			Term term = queryGraph.getTerm(v);
//			log.debug("ground term: " + term.toString());
			
			if (queryGraph.inDegreeOf(v) > 0) {
				String cacheString = iso.getVertexCorrespondence(v, true).toString() + "__" + term.toString();
				if (m_groundTermCache.containsKey(cacheString)) {
					if (m_groundTermCache.get(cacheString) == Boolean.FALSE) {
//						log.debug("--> not found (cache)");
						return null;
					}
					else {
//						log.debug("--> found (cache)");
						continue;
					}
				}

				Set<String> labels = new HashSet<String>();
				for (LabeledEdge<String> e : queryGraph.incomingEdgesOf(v)) {
					labels.add(e.getLabel());
				}
				
				Set<Triple> triples = new HashSet<Triple>();
				for (String property : labels) {
//					start(DATA);
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(v, true)).getTriples(property, term.toString());
//					end(DATA);
//					log.debug("  " + propTriples);
					if (propTriples.size() == 0) {
//						log.debug("not all ground terms found");
						m_groundTermCache.put(cacheString, false);
						return null;
					}
					triples.addAll(propTriples);
				}
				tripleMapping.put(v, new TripleSet(triples));
				
				m_groundTermCache.put(cacheString, true);
			}
			else {
				Set<Triple> triples = null;
				for (LabeledEdge<String> edge : queryGraph.outgoingEdgesOf(v)) {
					
//					start(DATA);
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(edge.getDst(), true)).getTriples(edge.getLabel());
//					end(DATA);
					if (triples == null) {
						triples = new HashSet<Triple>();
						for (Triple t : propTriples) {
							if (t.getSubject().equals(v))
								triples.add(new Triple("", "", t.getSubject()));
						}
					}
					else {
						Set<Triple> newTriples = new HashSet<Triple>();
						for (Triple t : propTriples) {
							if (t.getSubject().equals(v)) {
								Triple nt = new Triple("", "", t.getSubject());
								newTriples.add(nt);
							}
						}
						triples.retainAll(newTriples);
					}
				}
				if (triples == null || triples.size() == 0) {
//					log.debug("not all ground terms found 2");
					return null;
				}
				tripleMapping.put(v, new TripleSet(triples));
			}
//			log.debug("--> found");
		}
		
		edges.addAll(queryGraph.edgeSet());
		
		boolean done = false;
		int x = 0;
		while (!done) {
			LabeledQueryEdge<String> queryEdge = (LabeledQueryEdge<String>)edges.get(x);
			String source = queryEdge.getSrc();
			String target = queryEdge.getDst();
			String sourceUri = iso.getVertexCorrespondence(source, true);
			String targetUri = iso.getVertexCorrespondence(target, true);
			String propertyUri = queryEdge.getLabel();

			Term sourceTerm = queryGraph.getTerm(source);
			Term targetTerm = queryGraph.getTerm(target);
			
//			log.debug(sourceTerm + " " + propertyUri + " " + targetTerm);

			TripleSet targetTriples = tripleMapping.get(target);
			TripleSet sourceTriples = tripleMapping.get(source);
			
			boolean joinNecessary = true;
			
			if (targetTriples == null) {
				if (targetTerm instanceof Variable) {
//					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri));
//					end(DATA);
				}
				else if (targetTerm instanceof Individual) {
//					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri, targetTerm.toString()));
//					end(DATA);
				}
				else if (targetTerm instanceof Constant) {
//					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri, targetTerm.toString()));
//					end(DATA);
				}
			}
			
			if (sourceTriples == null) {
				if (queryGraph.inDegreeOf(source) > 0) {
					sourceTriples = new TripleSet();
					for (LabeledEdge<String> inEdge : queryGraph.incomingEdgesOf(source)) {
						if (sourceTerm instanceof Variable) {
//							start(DATA);
							sourceTriples.addTriples(m_em.extension(sourceUri).getTriples(inEdge.getLabel()));
//							end(DATA);
						}
						else if (sourceTerm instanceof Individual) {
//							start(DATA);
							Set<Triple> triples = m_em.extension(sourceUri).getTriples(inEdge.getLabel());
//							end(DATA);
							for (Triple t : triples)
								if (t.getObject().equals(sourceTerm.toString()))
									sourceTriples.addTriple(t);
						}
						else if (sourceTerm instanceof Constant) {
							throw new IllegalArgumentException("source term cannot be a constant");
						}
					}
				}
				else {
					if (!(sourceTerm instanceof Variable))
						throw new IllegalArgumentException("at this point the source term has to be a variable, if it was an individual it already should have been mapped");
					
					sourceTriples = new TripleSet();
//					for (Triple t : targetTriples.getTriples()) {
//						sourceTriples.addTriple(new Triple("", "", t.getSubject()));
//					}
					// TODO already finished, join is unnecessary
					log.debug("join not necessary");
					joinNecessary = false;
				}
			}
//			log.debug("sourceTriples: " + sourceTriples);
//			log.debug("targetTriples: " + targetTriples);
			

			List<TriplePair> triplePairs;
			if (joinNecessary)
				triplePairs = join(targetTriples, propertyUri, sourceTriples);
			else {
				triplePairs = new ArrayList<TriplePair>();
				for (Triple t : targetTriples.getTriples()) {
					Triple st = new Triple("", "", t.getSubject());
					sourceTriples.addTriple(st);
					st.addNext(t);
					t.addPrev(st);
					triplePairs.add(new TriplePair(t, st));
				}
//				log.debug("b " + triplePairs);
//				triplePairs = join(targetTriples, propertyUri, sourceTriples);
//				log.debug("a " + triplePairs);
			}

			if (triplePairs.size() == 0) {
				log.debug("join empty, no results, returning");
				return null;
			}
			
//			for (Triple t : sourceTriples.getTriples())
//				log.debug(t + " " + t.getPrev() + " " + t.getNext());
//			for (Triple t : targetTriples.getTriples())
//				log.debug(t + " " + t.getPrev() + " " + t.getNext());
			
			// check if the source already has mapped triples, if yes refine
			if (tripleMapping.get(source) == null) {
				TripleSet set = new TripleSet();
				for (TriplePair tp : triplePairs)
					set.addTriple(tp.source);
				tripleMapping.put(source, set);
			}
			else {
				// TODO refine mapped triples
//				log.debug("source refine");
				Set<String> objects = new HashSet<String>();
				for (TriplePair tp : triplePairs)
					objects.add(tp.source.getObject());
				tripleMapping.get(source).refineWith(objects);
			}
			
			// check if the target already has mapped triples, if yes refine
			if (tripleMapping.get(target) == null) {
//				TripleSet set = new TripleSet();
//				for (TriplePair tp : triplePairs)
//					set.addTriple(tp.target);
//				tripleMapping.put(target, set);
				tripleMapping.put(target, targetTriples);
			}
			else {
				// TODO refine mapped triples
//				log.debug("target refine");
				Set<String> objects = new HashSet<String>();
				for (TriplePair tp : triplePairs)
					objects.add(tp.target.getObject());
				tripleMapping.get(target).refineWith(objects);
			}
			
			x++;
			if (x >= edges.size())
				done = true;
		}
		
//		start(MAPPING);
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		TripleSet set = tripleMapping.get("?x");
		log.debug(set.getTriples());
		for (Triple t : set.getTriples()) {
			Set<Map<String,String>> mappings = createMappings("", queryGraph, "?x", new HashMap<String,String>(), t);
			for (Map<String,String> map : mappings)
				if (map.size() == tripleMapping.size())
					results.add(map);
//					log.debug("m: " + map);
		}
//		end(MAPPING);
		return results;
	}
	
	public Set<Map<String,String>> call() throws Exception {
//		System.out.println(m_iso);
		Set<Map<String,String>> set = validateMapping2(m_queryGraph, m_iso);
		return set;
	}

}
