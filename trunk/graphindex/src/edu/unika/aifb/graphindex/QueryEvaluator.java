package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.algorithm.DiGraphMatcher;
import edu.unika.aifb.graphindex.algorithm.EdgeLabelFeasibilityChecker;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.query.Constant;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.QueryVertex;
import edu.unika.aifb.graphindex.query.Term;
import edu.unika.aifb.graphindex.query.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class QueryEvaluator {
	public class TriplePair {
		public Triple target;
		public Triple source;
		
		public TriplePair(Triple target, Triple source) {
			this.target = target;
			this.source = source;
		}
		
		public String toString() {
			return target + "," + source;
		}
	}
	
	private class GraphTriple {
		public String src;
		public LabeledQueryEdge<String> edge;
		public String trg;
		
		public GraphTriple(String src, LabeledQueryEdge<String> edge, String target) {
			super();
			this.src = src;
			this.edge = edge;
			this.trg = target;
		}
	}
	
	public class TripleSet {
		private Set<Triple> m_triples;
		
		public TripleSet() {
			this(new HashSet<Triple>());
		}
		
		public TripleSet(Set<Triple> triples) {
			m_triples = triples;
		}
		
		public Set<Triple> getTriples() {
			return m_triples;
		}
		
		public void addTriples(Set<Triple> triples) {
			m_triples.addAll(triples);
		}
		
		public Set<String> getSubjects() {
			Set<String> subjects = new HashSet<String>();
			for (Triple t : m_triples)
				subjects.add(t.getSubject());
			return subjects;
		}
		
		public Set<String> getObjects() {
			Set<String> objects = new HashSet<String>();
			for (Triple t : m_triples)
				objects.add(t.getObject());
			return objects;
		}
		
		public void refineWith(Set<String> objects) {
			for (Iterator<Triple> i = iterator(); i.hasNext(); ) {
				if (!objects.contains(i.next().getObject()))
					i.remove();
			}
		}
		
		public void addToMapping(Set<String> mapped, Set<Map<String,String>> mappings, String term, List<String> sources, List<String> targets, Map<String,TripleSet> tripleMappings) {
			List<String> mappedSources = new ArrayList<String>();
			for (String s : sources)
				if (mapped.contains(s))
					mappedSources.add(s);
			
			List<String> mappedTargets = new ArrayList<String>();
			for (String t : targets)
				if (mapped.contains(t))
					mappedTargets.add(t);
			
			if (sources.size() == 0 && mappings.size() == 0) {
				for (String o : getObjects()) {
					Map<String,String> map = new HashMap<String,String>();
					map.put(term, o);
					mappings.add(map);
				}
			}
			else if (mappedSources.size() == 0 && mappings.size() > 0) {
//				log.debug("sources: " + sources + ", targets: " + targets);
				
				if (mappedTargets.size() == 0) {
					log.error("at least one target should be mapped");
				}
				else {
					Set<Map<String,String>> newMappings = new HashSet<Map<String,String>>();
					for (Iterator<Map<String,String>> i = mappings.iterator(); i.hasNext(); ) {
						Map<String,String> mapping = i.next();
//						log.debug("mapping: " + mapping);
						List<Triple> toAdd = new ArrayList<Triple>();
						for (Triple t : m_triples) {
//							log.debug(" t: " + t);
							for (String target : mappedTargets) {
								String targetMapped = mapping.get(target);
//								log.debug("  target: " + target + ", targetMapped: " + targetMapped);
								for (Triple targetTriple : tripleMappings.get(target).getTriples()) {
									if (targetTriple.getObject().equals(targetMapped) && targetTriple.getSubject().equals(t.getObject())) {
//										log.debug("   " + targetTriple + " valid");
										if (!mapping.values().contains(targetTriple.getSubject())) {
											toAdd.add(t);
											break;
										}
									}
								}
							}
						}
//						log.debug("toAdd: " + toAdd);
						i.remove();
						
						if (toAdd.size() > 0) {
							for (Triple t : toAdd) {
								Map<String,String> newMap = new HashMap<String,String>(mapping);
								newMap.put(term, t.getObject());
								newMappings.add(newMap);
							}
						}
					}
					mappings.addAll(newMappings);
				}
			}
			else if (mappedSources.size() > 0) {
				Map<String,List<Triple>> obj2t = new HashMap<String,List<Triple>>();
				for (Triple t : m_triples) {
					List<Triple> ts = obj2t.get(t.getObject());
					if (ts == null) {
						ts = new ArrayList<Triple>();
						obj2t.put(t.getObject(), ts);
					}
					ts.add(t);
				}
				
				Set<Map<String,String>> newMappings = new HashSet<Map<String,String>>();
				for (Iterator<Map<String,String>> j = mappings.iterator(); j.hasNext(); ) {
					Map<String,String> mapping = j.next();
					
//					log.debug("mapping: " + mapping);
					List<Triple> toAdd = new ArrayList<Triple>();
					if (sources.size() > 1) {
						List<String> sourcesMapped = new ArrayList<String>();
						for (String sourceV : mappedSources)
							sourcesMapped.add(mapping.get(sourceV));
//						log.debug("sourcesMapped: " + sourcesMapped);
						
						for (List<Triple> ts : obj2t.values()) {
//							log.debug("ts: " + ts);
							if (ts.size() < sourcesMapped.size())
								continue;
							
							boolean all = true;
							for (String sourceMapped : sourcesMapped) {
								boolean found = false;
								for (Triple t : ts) {
									if (t.getSubject().equals(sourceMapped)) {
										found = true;
										break;
									}
								}
								if (!found) {
									all = false;
									break;
								}
							}
//							log.debug("all: " + all);
							if (all)
								toAdd.add(ts.get(0));
						}
					}
					else {
						String sourceMapped = mapping.get(sources.get(0));
//						log.debug("sourceV: " + sources.get(0) + ", sourceMapped: " + sourceMapped);
						for (Triple t : m_triples) {
							if (t.getSubject().equals(sourceMapped)) {
//								log.debug("t to add: " + t);
								toAdd.add(t);
							}
						}
					}
					
					j.remove();
					
					if (toAdd.size() > 0) {
						for (Triple t : toAdd) {
							Map<String,String> newMap = new HashMap<String,String>(mapping);
							newMap.put(term, t.getObject());
							newMappings.add(newMap);
						}
					}
					
//					mapping.put(term, toAdd.get(0).getObject());
				}
//				log.debug("newMappings: " + newMappings);
				mappings.addAll(newMappings);
//				log.debug("mappings: " + mappings);
			}
		}
		
		public Iterator<Triple> iterator() {
			return m_triples.iterator();
		}

		public void addTriple(Triple triple) {
			m_triples.add(triple);
		}

		public int size() {
			return m_triples.size();
		}
		
		public String toString() {
			return m_triples.toString();
		}
	}
	
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	private Map<String,Boolean> m_groundTerms;
	private static final Logger log = Logger.getLogger(QueryEvaluator.class);
	private long[] starts = new long[10];
	private long[] timings = new long[10];
	private final  int DATA = 0, JOIN = 1, MAPPING = 2, RS = 3, MATCH = 4;
	
	public QueryEvaluator() {
		m_groundTerms = new HashMap<String,Boolean>();
	}
	
	private void start(int timer) {
		starts[timer] = System.currentTimeMillis();
	}
	
	private void end(int timer) {
		timings[timer] += System.currentTimeMillis() - starts[timer];
	}
	
	private ResultSet toResultSet(Set<Map<String,String>> mappings, Set<String> vars) {
		start(RS);
		ResultSet rs = new ResultSet(vars.toArray(new String[]{}));
		Set<String> toRemove = null;
		
		for (Map<String,String> map : mappings) {
			if (toRemove == null) {
				toRemove = new HashSet<String>();
				for (String key : map.keySet())
					if (!vars.contains(key))
						toRemove.add(key);
			}
			for (String key : toRemove)
				map.remove(key);
			rs.addResult(new Result(map));
		}
		end(RS);
		return rs;
	}
	
	private Set<Map<String,String>> createMappings(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, Map<String,TripleSet> tripleMapping) {
		start(MAPPING);
		
		Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
		
		Set<String> notVisited = new HashSet<String>(queryGraph.vertexSet());
		Set<String> visited = new HashSet<String>();
		
		List<String> startCandidates = new ArrayList<String>();
		for (String qv : queryGraph.vertexSet()) {
			if (queryGraph.inDegreeOf(qv) == 0) 
				startCandidates.add(qv);
		}
		
		// TODO handle queries with no vertices with no incoming edges
		while (notVisited.size() > 0) {
			String startVertex = startCandidates.remove(0);
			Stack<String> toVisit = new Stack<String>();
			toVisit.add(startVertex);
			
			while (toVisit.size() != 0) {
				String currentVertex = toVisit.pop();
				if (visited.contains(currentVertex))
					continue;
				
				notVisited.remove(currentVertex);
//				log.debug("visiting: " + currentVertex);
				
				List<String> sources = new ArrayList<String>();
				for (LabeledEdge<String> inEdge : queryGraph.incomingEdgesOf(currentVertex)) {
					sources.add(inEdge.getSrc());
					if (!visited.contains(inEdge.getSrc()))
						toVisit.add(inEdge.getSrc());
				}
				
				List<String> targets = new ArrayList<String>();
				for (LabeledEdge<String> outEdge : queryGraph.outgoingEdgesOf(currentVertex)) {
					targets.add(outEdge.getDst());
					if (!visited.contains(outEdge.getDst()))
						toVisit.add(outEdge.getDst());
				}
				
				tripleMapping.get(currentVertex).addToMapping(visited, mappings, currentVertex, sources, targets, tripleMapping);
				
				visited.add(currentVertex);
			}
		}
//		log.debug("mappings: " + mappings);
		end(MAPPING);
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
		start(JOIN);
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
		
		end(JOIN);
		return result;
	}
	
	private void validateMapping(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso) throws StorageException {
		Set<String> notVisited = new HashSet<String>(queryGraph.vertexSet());
		
		List<String> startCandidates = new ArrayList<String>();
		for (String qv : queryGraph.vertexSet()) {
			if (queryGraph.inDegreeOf(qv) == 0) 
				startCandidates.add(qv);
		}
		
		// TODO handle queries with no vertices with no incoming edges
		
		String startVertex = startCandidates.remove(0);
		Stack<String> toVisit = new Stack<String>();
		toVisit.add(startVertex);
		
		Map<String,Set<Triple>> tripleMapping = new HashMap<String,Set<Triple>>();
		
		while (toVisit.size() != 0) {
			String sourceVertex = toVisit.pop();
			notVisited.remove(sourceVertex);
			
			Term sourceTerm = queryGraph.getTerm(sourceVertex);
			String sourceExt = iso.getVertexCorrespondence(sourceVertex, true);
			
			for (LabeledEdge<String> edge : queryGraph.outgoingEdgesOf(sourceVertex)) {
				String propertyUri = edge.getLabel();
				String targetVertex = edge.getDst();
				Term targetTerm = queryGraph.getTerm(targetVertex);
				String targetExt = iso.getVertexCorrespondence(targetVertex, true);
				
				log.debug(sourceVertex + "(" + sourceExt + ")" + " " + propertyUri + " " + targetVertex + "(" + targetExt + ")");
				
				Set<Triple> sourceTriples = tripleMapping.get(sourceVertex);
				Set<Triple> targetTriples = tripleMapping.get(targetVertex);
				
				if (targetTriples == null) {
					if (targetTerm instanceof Variable) {
						targetTriples = m_em.extension(targetExt).getTriples(propertyUri);
					}
					else if (targetTerm instanceof Individual) {
						targetTriples = m_em.extension(targetExt).getTriples(propertyUri, targetTerm.toString());
					}
					else if (targetTerm instanceof Constant) {
						targetTriples = m_em.extension(targetExt).getTriples(propertyUri, targetTerm.toString());
					}
				}
				
				if (sourceTriples == null) {
					if (queryGraph.inDegreeOf(sourceVertex) > 0) {
						if (sourceTerm instanceof Variable) {
							sourceTriples = m_em.extension(sourceExt).getTriples();
						}
						else if (sourceTerm instanceof Individual) {
							sourceTriples = m_em.extension(sourceExt).getTriples();
							for (Iterator<Triple> i = sourceTriples.iterator(); i.hasNext(); )
								if (!i.next().getObject().equals(sourceTerm.toString()))
									i.remove();
						}
						else if (sourceTerm instanceof Constant) {
							throw new IllegalArgumentException("source term cannot be a constant");
						}
					}
					else {
						if (!(sourceTerm instanceof Variable))
							throw new IllegalArgumentException("at this point the source term has to be a variable, if it was an individual it already should have been mapped");
						
						sourceTriples = new HashSet<Triple>();
						for (Triple t : targetTriples) {
							sourceTriples.add(new Triple("", "", t.getSubject()));
						}
						// TODO already finished, join is unnecessary
					}
				}
				
//				log.debug("source triples: " + sourceTriples);
//				log.debug("target triples: " + targetTriples);
			}
		}
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
	
	private Set<Map<String,String>> validateMapping2(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso) throws StorageException {
		
		List<LabeledEdge<String>> edges = new ArrayList<LabeledEdge<String>>();
		
		Map<String,TripleSet> tripleMapping = new HashMap<String,TripleSet>();
		
		for (String v : queryGraph.getGroundTerms()) {
			Term term = queryGraph.getTerm(v);
//			log.debug("ground term: " + term.toString());
			
			if (queryGraph.inDegreeOf(v) > 0) {
				String cacheString = iso.getVertexCorrespondence(v, true).toString() + "__" + term.toString();
				if (m_groundTerms.containsKey(cacheString)) {
					if (m_groundTerms.get(cacheString) == Boolean.FALSE) {
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
					start(DATA);
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(v, true)).getTriples(property, term.toString());
					end(DATA);
//					log.debug("  " + propTriples);
					if (propTriples.size() == 0) {
//						log.debug("not all ground terms found");
						m_groundTerms.put(cacheString, false);
						return null;
					}
					triples.addAll(propTriples);
				}
				tripleMapping.put(v, new TripleSet(triples));
				
				m_groundTerms.put(cacheString, true);
			}
			else {
				Set<Triple> triples = null;
				for (LabeledEdge<String> edge : queryGraph.outgoingEdgesOf(v)) {
					
					start(DATA);
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(edge.getDst(), true)).getTriples(edge.getLabel());
					end(DATA);
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
			
			if (targetTriples == null) {
				if (targetTerm instanceof Variable) {
					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri));
					end(DATA);
				}
				else if (targetTerm instanceof Individual) {
					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri, targetTerm.toString()));
					end(DATA);
				}
				else if (targetTerm instanceof Constant) {
					start(DATA);
					targetTriples = new TripleSet(m_em.extension(targetUri).getTriples(propertyUri, targetTerm.toString()));
					end(DATA);
				}
			}
			
			if (sourceTriples == null) {
				if (queryGraph.inDegreeOf(source) > 0) {
					sourceTriples = new TripleSet();
					for (LabeledEdge<String> inEdge : queryGraph.incomingEdgesOf(source)) {
						if (sourceTerm instanceof Variable) {
							start(DATA);
							sourceTriples.addTriples(m_em.extension(sourceUri).getTriples(inEdge.getLabel()));
							end(DATA);
						}
						else if (sourceTerm instanceof Individual) {
							start(DATA);
							Set<Triple> triples = m_em.extension(sourceUri).getTriples(inEdge.getLabel());
							end(DATA);
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
					for (Triple t : targetTriples.getTriples()) {
						sourceTriples.addTriple(new Triple("", "", t.getSubject()));
					}
					// TODO already finished, join is unnecessary
					log.debug("join not necessary");
				}
			}
//			log.debug("sourceTriples: " + sourceTriples);
//			log.debug("targetTriples: " + targetTriples);
			

			List<TriplePair> triplePairs = join(targetTriples, propertyUri, sourceTriples);

			if (triplePairs.size() == 0) {
//				log.debug("join empty, no results, returning");
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
		
		start(MAPPING);
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		TripleSet set = tripleMapping.get("?x");
//		log.debug(set.getTriples());
		for (Triple t : set.getTriples()) {
			Set<Map<String,String>> mappings = createMappings("", queryGraph, "?x", new HashMap<String,String>(), t);
			for (Map<String,String> map : mappings)
				if (map.size() == tripleMapping.size())
					results.add(map);
//					log.debug("m: " + map);
		}
		end(MAPPING);

//		TripleSet set = tripleMapping.get("?x");
//		log.debug(set.getTriples());
//		Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
//		set.addToMapping(mappings, "?x", new ArrayList<String>(), Arrays.asList(new String[]{"?y", "?p"}), null);
//		
//		set = tripleMapping.get("?y");
//		log.debug(set.getTriples());
//		set.addToMapping(mappings, "?y", Arrays.asList(new String[]{"?x"}), Arrays.asList(new String[]{"?p"}), null);
//
//		set = tripleMapping.get("?p");
//		log.debug(set.getTriples());
//		set.addToMapping(mappings, "?p", Arrays.asList(new String[]{"?x", "?y"}), new ArrayList<String>(), null);
		
//		Set<Map<String,String>> mappings = createMappings(queryGraph, tripleMapping);
//		for (Map<String,String> map : mappings)
//			log.debug("m2: " + map);
		return results;
//		return null;
	}
	
	public ResultSet evaluate(Query query, StructureIndex index) throws StorageException {
		long start = System.currentTimeMillis();
		
		NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		log.debug(queryGraph);
		Util.printDOT("query.dot", queryGraph);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		int i = 0;
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		for (NamedGraph<String,LabeledEdge<String>> indexGraph : index.getIndexGraphs()) {
			log.debug(indexGraph);
			Util.printDOT(indexGraph);
			
			start(MATCH);
			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(queryGraph, indexGraph, true, new EdgeLabelFeasibilityChecker());
			
			if (!matcher.isSubgraphIsomorphic()) {
				end(MATCH);
				continue;
			}
			end(MATCH);
			
			log.debug("mappings found: " + matcher.numberOfMappings());
			
			for (IsomorphismRelation<String,LabeledEdge<String>> iso : matcher) {
//				log.debug("----------------------------------------------");
//				log.debug("mapping: " + iso);
				Set<Map<String,String>> result = validateMapping2(queryGraph, iso);
				if (result != null)
					results.addAll(result);
			}
		}
		log.debug("result maps: " + results.size());
		
		long end = System.currentTimeMillis();
		log.info((end - start) / 1000.0);
		m_groundTerms.clear();
		
		ResultSet rs = toResultSet(results, queryGraph.getVariables());
		log.debug(rs);
		log.debug("size: " + rs.size());
		
		
		log.debug("time spent");
		log.debug(" subgraph matching: " + (timings[MATCH] / 1000.0));
		log.debug(" retrieving data: " + (timings[DATA] / 1000.0));
		log.debug(" joining: " + (timings[JOIN] / 1000.0));
		log.debug(" computing mappings: " + (timings[MAPPING] / 1000.0));
		log.debug(" building result set: " + (timings[RS] / 1000.0));
		return null;
	}
}
