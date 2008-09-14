package edu.unika.aifb.graphindex;

import java.util.ArrayList;
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
		public Triple left;
		public Triple right;
		
		public TriplePair(Triple left, Triple right) {
			this.left = left;
			this.right = right;
		}
		
		public String toString() {
			return left + "," + right;
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
	
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	
	private static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public QueryEvaluator() {
	}
	
	private ResultSet toResultSet(Set<Map<String,String>> mappings, String[] vars) {
		ResultSet rs = new ResultSet(vars);
		
		for (Map<String,String> map : mappings) {
			Object[] data = new Object[vars.length];
			for (int i = 0; i < vars.length; i++) {
				data[i] = map.get(vars[i]);
			}
			rs.addResult(data);
		}
		return rs;
	}
	
	/**
	 * left.subject = right.object
	 * 
	 * @param left
	 * @param leftProperty
	 * @param right
	 * @return
	 * @throws StorageException
	 */
	public List<TriplePair> join(Set<Triple> leftTriples, String leftProperty, Set<Triple> rightTriples) throws StorageException {
		
		// TODO use smaller triple set to create hash table (can't just swap, because we use
		// different columns on each side)
		
		Map<String,List<Triple>> leftSubject2Triples = new HashMap<String,List<Triple>>();
		for (Triple lt : leftTriples) {
			List<Triple> triples = leftSubject2Triples.get(lt.getSubject());
			if (triples == null) {
				triples = new ArrayList<Triple>();
				leftSubject2Triples.put(lt.getSubject(), triples);
			}
			triples.add(lt);
		}
		
		List<TriplePair> result = new LinkedList<TriplePair>();
		
		for (Triple rt : rightTriples) {
			List<Triple> lts = leftSubject2Triples.get(rt.getObject());
			if (lts != null && lts.size() > 0) {
				for (Triple lt : lts) {
					result.add(new TriplePair(lt, rt));
				}
			}
		}
		
		return result;
	}
	
	private class ResultMapping {
		private Map<String,String> m_map;
		
		public ResultMapping() {
			m_map = new HashMap<String,String>();
		}
		
		public ResultMapping(ResultMapping mapping) {
			m_map = new HashMap<String,String>(mapping.getMap());
		}
		
		public Map<String,String> getMap() {
			return m_map;
		}
		
		public boolean isMapped(String term) {
			return m_map.containsKey(term);
		}
		
		public String getMapped(String term) {
			return m_map.get(term);
		}
		
		public void map(String term, String val) {
			m_map.put(term, val);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_map == null) ? 0 : m_map.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ResultMapping other = (ResultMapping)obj;
			if (m_map == null) {
				if (other.m_map != null)
					return false;
			} else if (!m_map.equals(other.m_map))
				return false;
			return true;
		}
	}
	
	private Set<Map<String,String>> validateMapping2(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso) throws StorageException {
		
		Set<ResultMapping> results = new HashSet<ResultMapping>();

		String startVertex = "?x";
		
		List<LabeledEdge<String>> edges = new ArrayList<LabeledEdge<String>>();
		
		Map<String,Set<Triple>> tripleMapping = new HashMap<String,Set<Triple>>();
		
		for (String v : queryGraph.getGroundTerms()) {
			Term term = queryGraph.getTerm(v);
			log.debug("ground term: " + term.toString());

			if (queryGraph.inDegreeOf(v) > 0) {
				Set<String> labels = new HashSet<String>();
				for (LabeledEdge<String> e : queryGraph.incomingEdgesOf(v)) {
					labels.add(e.getLabel());
				}
				
				Set<Triple> triples = new HashSet<Triple>();
				for (String property : labels) {
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(v, false)).getTriples(property, term.toString());
//					log.debug("  " + propTriples);
					if (propTriples.size() == 0) {
						log.debug("not all ground terms found");
						return null;
					}
					triples.addAll(propTriples);
				}
				tripleMapping.put(v, triples);
			}
			else {
				Set<Triple> triples = null;
				for (LabeledEdge<String> edge : queryGraph.outgoingEdgesOf(v)) {
					Set<Triple> propTriples = m_em.extension(iso.getVertexCorrespondence(edge.getDst(), false)).getTriples(edge.getLabel());
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
					log.debug("not all ground terms found 2");
					return null;
				}
				tripleMapping.put(v, triples);
			}
			log.debug("--> found");
		}
		
		edges.addAll(queryGraph.edgeSet());
		
		boolean done = false;
		int x = 0;
		while (!done) {
			LabeledQueryEdge<String> queryEdge = (LabeledQueryEdge<String>)edges.get(x);
			String source = queryEdge.getSrc();
			String target = queryEdge.getDst();
			String sourceUri = iso.getVertexCorrespondence(source, false);
			String targetUri = iso.getVertexCorrespondence(target, false);
			String propertyUri = queryEdge.getLabel();

			Term sourceTerm = queryGraph.getTerm(source);
			Term targetTerm = queryGraph.getTerm(target);
			
			log.debug(sourceTerm + " " + propertyUri + " " + targetTerm);

			Set<Triple> targetTriples = tripleMapping.get(target);
			Set<Triple> sourceTriples = tripleMapping.get(source);
			
			if (targetTriples == null) {
				if (targetTerm instanceof Variable) {
					targetTriples = m_em.extension(targetUri).getTriples(propertyUri);
				}
				else if (targetTerm instanceof Individual) {
					targetTriples = m_em.extension(targetUri).getTriples(propertyUri, targetTerm.toString());
				}
				else if (targetTerm instanceof Constant) {
					
				}
			}
			
			if (sourceTriples == null) {
				if (queryGraph.inDegreeOf(source) > 0) {
					if (sourceTerm instanceof Variable) {
						sourceTriples = m_em.extension(sourceUri).getTriples();
					}
					else if (sourceTerm instanceof Individual) {
						sourceTriples = m_em.extension(sourceUri).getTriples();
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
					// TODO already finished, join is be unnecessary
				}
			}
//			log.debug("sourceTriples: " + sourceTriples);
//			log.debug("targetTriples: " + targetTriples);
			List<TriplePair> triplePairs = join(targetTriples, propertyUri, sourceTriples);
//			log.debug(triplePairs);
//			for (TriplePair tp : triplePairs) {
//				log.debug("-->" + tp.right.getObject() + " " + propertyUri + " " + tp.left.getObject());
//			}
			if (triplePairs.size() == 0) {
				log.debug("join empty, returning");
				return null;
			}
			
			for (String term : tripleMapping.keySet()) {
				Set<Triple> mappedTriples = tripleMapping.get(term);
				System.out.print(term + ":" + mappedTriples.size() + " ");
			}
			System.out.println();
			
			if (tripleMapping.get(source) == null) {
				Set<Triple> triples = new HashSet<Triple>();
				for (TriplePair tp : triplePairs) {
					triples.add(tp.right);
				}
				tripleMapping.put(source, triples);
			}
			else {
				// TODO refine mapped triples
				log.debug("source refine");
				Set<String> objects = new HashSet<String>();
				for (TriplePair tp : triplePairs) {
					objects.add(tp.right.getObject());
				}
				log.debug("source " + source + " objects: " + objects.size());
				log.debug(tripleMapping.get(source).size());
				for (Iterator<Triple> i = tripleMapping.get(source).iterator(); i.hasNext(); ) {
					Triple t = i.next();
					if (!objects.contains(t.getObject())) {
//						log.debug("removing " + t);
						i.remove();
					}
				}
				log.debug(tripleMapping.get(source).size());
			}
			
			if (tripleMapping.get(target) == null) {
				Set<Triple> triples = new HashSet<Triple>();
				for (TriplePair tp : triplePairs) {
					triples.add(tp.left);
				}
				tripleMapping.put(target, triples);
			}
			else {
				// TODO refine mapped triples
				log.debug("target refine");
				Set<String> objects = new HashSet<String>();
				for (TriplePair tp : triplePairs) {
					objects.add(tp.left.getObject());
				}
				log.debug("target objects: " + objects.size());
				log.debug(tripleMapping.get(target).size());
				for (Iterator<Triple> i = tripleMapping.get(target).iterator(); i.hasNext(); ) {
					Triple t = i.next();
					if (!objects.contains(t.getObject()))
						i.remove();
				}
				log.debug(tripleMapping.get(target).size());
			}
			
			x++;
			if (x >= edges.size())
				done = true;
		}
		
//		log.debug(tripleMapping);
		for (String term : tripleMapping.keySet()) {
			Set<Triple> mappedTriples = tripleMapping.get(term);
			System.out.print(term + ":" + mappedTriples.size() + " ");
		}
		System.out.println();
		
//		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		
		return null;
	}
	
	public void evaluate(Query query, StructureIndex index) throws StorageException {
		long start = System.currentTimeMillis();
		
		NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		log.debug(queryGraph);
		Util.printDOT("query.dot", queryGraph);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		int i = 0;
		for (NamedGraph<String,LabeledEdge<String>> indexGraph : index.getIndexGraphs()) {
			log.debug(indexGraph);
			
			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(queryGraph, indexGraph, true, new EdgeLabelFeasibilityChecker());
			
			if (!matcher.isSubgraphIsomorphic())
				continue;
			
			log.debug("mappings found: " + matcher.numberOfMappings());
			
			Set<Map<String,String>> results = new HashSet<Map<String,String>>();
			int totalResults = 0;
			for (IsomorphismRelation<String,LabeledEdge<String>> iso : matcher) {
				log.debug("----------------------------------------------");
				log.debug("mapping: " + iso);
				Set<Map<String,String>> result = validateMapping2(queryGraph, iso);
			}
			log.debug("total results: " + totalResults);
			log.debug("result maps: " + results.size());

//			ResultSet rs = toResultSet(results, ((QueryGraph)qg).getVariables());
		}
		
		long end = System.currentTimeMillis();
		log.info((end - start) / 1000.0);
	}
}
