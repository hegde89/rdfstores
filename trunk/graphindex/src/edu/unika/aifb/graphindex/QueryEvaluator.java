package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.algorithm.Matching;
import edu.unika.aifb.graphindex.algorithm.NaiveSubgraphMatcher;
import edu.unika.aifb.graphindex.algorithm.Partitioner;
import edu.unika.aifb.graphindex.algorithm.SubgraphMatcher;
import edu.unika.aifb.graphindex.extensions.ExtEntry;
import edu.unika.aifb.graphindex.extensions.Extension;
import edu.unika.aifb.graphindex.extensions.ExtensionManager;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphElement;
import edu.unika.aifb.graphindex.graph.Path;
import edu.unika.aifb.graphindex.graph.Vertex;
import edu.unika.aifb.graphindex.query.Constant;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.QueryVertex;
import edu.unika.aifb.graphindex.query.Term;
import edu.unika.aifb.graphindex.query.Variable;

public class QueryEvaluator {
	private class Triple {
		public QueryVertex src;
		public Edge edge;
		public QueryVertex trg;
		
		public Triple(QueryVertex src, Edge edge, QueryVertex target) {
			super();
			this.src = src;
			this.edge = edge;
			this.trg = target;
		}
	}
	
	private SubgraphMatcher m_sm;
	private ExtensionManager m_em = ExtensionManager.getInstance();
	
	private static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public QueryEvaluator(SubgraphMatcher sm) {
		m_sm = sm;
	}
	
	/**
	 * Recursively builds the result set, using the specified mapping of query graph nodes to dataguide nodes.
	 * 
	 * @param currentVertex
	 * @param results
	 * @param mapping
	 * @return
	 */
	private Set<Map<String,String>> validateMapping(QueryVertex startVertex, Map<String,String> graphMapping) {

		Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
		
		Stack<Triple> stack = new Stack<Triple>();
		stack.push(new Triple(null, null, startVertex));
		
		while (stack.size() != 0) {
			Triple t = stack.pop();
			
			String extUri = graphMapping.get(t.trg.getLabel());
			Extension ext = m_em.getExtension(extUri);
			Term term = t.trg.getTerm();
			
			if (t.edge == null) {
				// starting vertex
				// if mapped to an individual, check if individual in extension and add mapping
				// otherwise, add mapping for all uris in extension
				if (term instanceof Individual) {
					if (!ext.containsUri(term.toString())) // graph mapping invalid, return
						break;

					Map<String,String> map = new HashMap<String,String>();
					map.put(t.trg.getLabel(), term.toString());
					mappings.add(map);
				}
				else {
					for (String uri : ext.getUris()) {
						Map<String,String> map = new HashMap<String,String>();
						map.put(t.trg.getLabel(), uri);
						mappings.add(map);
					}
				}
				
				// TODO should never be a constant here
			}
			else {
				// t is incoming triple
				// remove all mappings where the preceding vertex (t.src) is not
				// mapped to a parent of an uri in the extension of the current vertex
				// along the edge t.edge
				// add to mappings or duplicate mappings for uris in current
				// extension which have a parent in the last extension along
				// edge t.edge
				
				Set<Map<String,String>> newMappings = new HashSet<Map<String,String>>();
				for (Iterator<Map<String,String>> i = mappings.iterator(); i.hasNext(); ) {
					Map<String,String> map = i.next();
					String mappedParentUri = map.get(t.src.getLabel());
					
					if (term instanceof Individual) {
						// all mappings are already checked for ground terms

						// remove current mapping, if parent vertex is not mapped 
						// to a parent of the individual uri
						if (!ext.getParents(term.toString(), t.edge.getLabel()).contains(mappedParentUri)) {
							i.remove();
							continue;
						}

						// mapped parent is in fact parent of individual, just add
						// mapping for individual
						Map<String,String> newMap = new HashMap<String,String>(map);
						newMap.put(t.trg.getLabel(), term.toString());
						newMappings.add(newMap);
						
						continue;
					}
					
					Set<String> children = ext.getChildren(t.edge.getLabel(), mappedParentUri);
					
					for (Iterator<String> j = children.iterator(); j.hasNext(); ) {
						String childUri = j.next();
						if (!ext.containsUri(childUri, t.edge.getLabel())) {
							j.remove();
						}
					}
					
					if (children.size() == 0) {
						// mapping of parent produces no children contained in current extension
						// -> mapping invalid
						i.remove();
						continue;
					}
					
					if (term instanceof Variable) {
						// add mappings for current vertex
						// if a parent has more than one child in the current extension,
						// duplicate mappings accordingly
						for (String childUri : children) {
							// checks if current vertex is already mapped to a different value,
							// which makes the mapping invalid
							if (map.containsKey(t.trg.getLabel()) && !map.get(t.trg.getLabel()).equals(childUri)) {
								i.remove();
								continue;
							}
							
							Map<String,String> newMap = new HashMap<String,String>(map);
							newMap.put(t.trg.getLabel(), childUri);
							newMappings.add(newMap);
						}
					}
					else if (term instanceof Constant) {
						// TODO constant
					}
					else {
						log.error("unknown term type");
					}
				}
				mappings = newMappings;
			}
			
			for (Edge e : t.trg.outgoingEdges()) {
				Triple out = new Triple(t.trg, e, (QueryVertex)e.getTarget());
				stack.push(out);
			}
		}
		
//		for (Map<String,String> map : mappings) 
//			log.info(map);
		log.debug(mappings.size());
		return mappings;
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
	
	public void evaluate(Query query, Index index) {
		long start = System.currentTimeMillis();
		
		QueryGraph queryGraph = query.toQueryGraph();

		Util.printDOT("query.dot", queryGraph);
		
		Partitioner partitioner = new Partitioner(queryGraph, QueryGraph.class.getCanonicalName());
		
		int i = 0;
		Graph qg;
		while ((qg = partitioner.nextPartition()) != null) {
			Util.printDOT("qg_" + i + ".dot", qg);
			for (Graph indexGraph : index.getIndexGraphs()) {
				Matching m = m_sm.match(indexGraph, qg);
				log.debug(m.getMappings().size());
	//			log.debug(m.getMappings());
				
				Set<Map<String,String>> results = new HashSet<Map<String,String>>();
				int totalResults = 0;
				boolean foundValidMapping = false;
				for (Map<String,String> mapping : m.getMappings()) {
					boolean groundTermsFound = true;
					for (QueryVertex v : ((QueryGraph)qg).getGroundTerms()) {
						Extension ext = m_em.getExtension(mapping.get(v.getLabel()));
						if (!ext.containsUri(v.getLabel())) {
							groundTermsFound = false;
							break;
						}
					}
					
					if (!groundTermsFound) {
						continue;
					}
					
					log.debug("all ground terms found");
					
					Set<Map<String,String>> result = validateMapping((QueryVertex)qg.getRoot(), mapping);
					totalResults += result.size();
					results.addAll(result);
				}
				log.debug("total results: " + totalResults);
				log.debug("result maps: " + results.size());
	
				ResultSet rs = toResultSet(results, ((QueryGraph)qg).getVariables());
				log.debug(rs.size());
				Join.printSet(rs);
			}
		}
		log.debug(m_em.stats());
		
		long end = System.currentTimeMillis();
		log.info((end - start) / 1000.0);
	}
}
