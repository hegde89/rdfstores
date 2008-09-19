package edu.unika.aifb.graphindex.graph.isomorphism;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;


/**
 * Implementation of the VF2 graph isomorphism algorithm, published in:
 <p>
 	Luigi P. Cordella, Pasquale Foggia, Carlo Sansone, Mario Vento,
	"A (Sub)Graph Isomorphism Algorithm for Matching Large Graphs,"
	IEEE Transactions on Pattern Analysis and Machine Intelligence,
	vol. 26,  no. 10,  pp. 1367-1372,  Oct.,  2004.
	<p>
 * Inspired by the implementation in the NetworkX graph library for Python: {@link https://networkx.lanl.gov/wiki}
 * 	 
 * @author gl
 *
 * @param <V>
 * @param <E>
 */
public class DiGraphMatcher<V,E> implements Iterator<IsomorphismRelation<V,E>>, Iterable<IsomorphismRelation<V,E>> {
	
	public class DiGMState {
		private V node1, node2;
		private int depth;
		
		public DiGMState(V n1, V n2) {
			node1 = null;
			node2 = null;
			depth = core1.keySet().size();
			
			if (n1 != null && n2 != null) {
				core1.put(n1, n2);
				core2.put(n2, n1);
				
				node1 = n1;
				node2 = n2;
				
				depth = core1.keySet().size();
	
				if (!in1.containsKey(n1))
					in1.put(n1, depth);
				if (!out1.containsKey(n1))
					out1.put(n1, depth);
				
				if (!in2.containsKey(n2))
					in2.put(n2, depth);
				if (!out2.containsKey(n2))
					out2.put(n2, depth);
				
				predUpdate(g1, core1, in1);
				predUpdate(g2, core2, in2);
				
				succUpdate(g1, core1, out1);
				succUpdate(g2, core2, out2);
			}
		}
		
		private void predUpdate(DirectedGraph<V,E> g, Map<V,V> core, Map<V,Integer> in) {
			Set<V> newNodes = new HashSet<V>();
			for (V n : core.keySet()) {
				for (V p : predecessors(g, n)) {
					if (!core.containsKey(p))
						newNodes.add(p);
				}
			}
			for (V n : newNodes) {
				if (!in.containsKey(n)) {
					in.put(n, depth);
				}
			}
		}
		
		private void succUpdate(DirectedGraph<V,E> g, Map<V,V> core, Map<V,Integer> out) {
			Set<V> newNodes = new HashSet<V>();
			for (V n : core.keySet()) {
				for (V p : successors(g, n)) {
					if (!core.containsKey(p))
						newNodes.add(p);
				}
			}
			for (V n : newNodes) {
				if (!out.containsKey(n)) {
					out.put(n, depth);
				}
			}
		}
		
		private void remove(Map<V,Integer> set) {
			for (Iterator<V> i = set.keySet().iterator(); i.hasNext(); ) {
				V n = i.next();
				if (set.get(n) == depth)
					i.remove();
			}
		}
		
		public void remove() {
			if (node1 != null && node2 != null) {
				core1.remove(node1);
				core2.remove(node2);
			}
	
			remove(in1);
			remove(in2);
			remove(out1);
			remove(out2);
		}
	}
	
	private class Pair {
		public V n1, n2;
		
		public Pair(V n1, V n2) {
			this.n1 = n1;
			this.n2 = n2;
		}
		
		public String toString() {
			return "(" + n1 + "," + n2 + ")";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((n1 == null) ? 0 : n1.hashCode());
			result = prime * result + ((n2 == null) ? 0 : n2.hashCode());
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
			Pair other = (Pair)obj;
			if (n1 == null) {
				if (other.n1 != null)
					return false;
			} else if (!n1.equals(other.n1))
				return false;
			if (n2 == null) {
				if (other.n2 != null)
					return false;
			} else if (!n2.equals(other.n2))
				return false;
			return true;
		}
	}
	
	private final int TEST_GRAPH = 0;
	private final int TEST_SUBGRAPH = 1;
	
	private DirectedGraph<V,E> g1, g2;
	private Map<V,V> core1, core2;
	private Map<V,Integer> in1, in2, out1, out2;
	private DiGMState m_state;
	private List<Map<V,V>> m_mappings;
	private List<IsomorphismRelation<V,E>> m_isomorphisms;
	private Iterator<IsomorphismRelation<V,E>> m_isomorphismIterator;
	private FeasibilityChecker<V,E,DirectedGraph<V,E>> m_checker;
	private MappingListener<V,E> m_listener;
	private int m_test;
	private boolean m_generateMappings;
	private final static Logger log = Logger.getLogger(DiGraphMatcher.class);
	
	/**
	 * Create a new matcher object. If testing for subgraph isomorphism, <code>graph1</code> should be the "smaller" graph. If
	 * <code>generateMappings</code> is true, all possible mappings are generated, which can take time. Otherwise, the algorithm
	 * stops as early as possible and does not generate any mappings.
	 * <p>
	 * This class also implements the Iterable interface, which is used to iterate over all mappings generated.
	 * 
	 * @param graph1
	 * @param graph2
	 * @param generateMappings wether to generate mappings or just check for isomorphisms 
	 */
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings) {
		g1 = graph1;
		g2 = graph2;
		
		core1 = new HashMap<V,V>();
		core2 = new HashMap<V,V>();
		in1 = new HashMap<V,Integer>();
		in2 = new HashMap<V,Integer>();
		out1 = new HashMap<V,Integer>();
		out2 = new HashMap<V,Integer>();
		
		m_state = new DiGMState(null, null);
		
		m_mappings = new ArrayList<Map<V,V>>();
		m_test = TEST_GRAPH;
		m_checker = new FeasibilityChecker<V,E,DirectedGraph<V,E>>() { // default checker, always returns true
			public boolean isEdgeCompatible(E e1, E e2) {
				return true;
			}

			public boolean isVertexCompatible(V n1, V n2) {
				return true;
			}
		};
		m_listener = null;
		m_generateMappings = generateMappings;
		
		g1preds = new HashMap<V,Set<V>>();
		g2preds = new HashMap<V,Set<V>>();
		g1succs = new HashMap<V,Set<V>>();
		g2succs = new HashMap<V,Set<V>>();
	}
	
	/**
	 * Create a new matcher object. If testing for subgraph isomorphism, <code>graph1</code> should be the "smaller" graph. If
	 * <code>generateMappings</code> is true, all possible mappings are generated, which can take time. Otherwise, the algorithm
	 * stops as early as possible and does not generate any mappings. The feasibility checker is used to determine semantic 
	 * feasibility.
	 * 
	 * @param graph1
	 * @param graph2
	 * @param generateMappings
	 * @param checker a FeasibilityChecker which is used to determine if the current mapping can be extended by the two nodes
	 */
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings, FeasibilityChecker<V,E,DirectedGraph<V,E>> checker) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
	}
	
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings, FeasibilityChecker<V,E,DirectedGraph<V,E>> checker, MappingListener<V,E> listener) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
		m_listener = listener;
	}
	
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings, MappingListener<V,E> listener) {
		this(graph1, graph2, generateMappings);
		m_listener = listener;
	}
	
	private Map<V,Set<V>> g1preds, g2preds, g1succs, g2succs;
	
	// TODO add predecessors and successors to graph class
	private Set<V> predecessors(DirectedGraph<V,E> g, V v) {
		Map<V,Set<V>> predCache;
		if (g == g1)
			predCache = g1preds;
		else
			predCache = g2preds;
		
		Set<V> preds = predCache.get(v);
		
		if (preds == null) {
			preds = new HashSet<V>();
			for (E edge : g.incomingEdgesOf(v)) {
				preds.add(g.getEdgeSource(edge));
			}
			predCache.put(v, preds);
		}

		return preds;
	}
	
	private Set<V> successors(DirectedGraph<V,E> g, V v) {
		Map<V,Set<V>> succCache;
		if (g == g1)
			succCache = g1succs;
		else
			succCache = g2succs;
		
		Set<V> succs = succCache.get(v);
		
		if (succs == null) {
			succs = new HashSet<V>();
			for (E edge : g.outgoingEdgesOf(v)) {
				succs.add(g.getEdgeTarget(edge));
			}
			succCache.put(v, succs);
		}

		return succs;
	}
	
	/**
	 * Determines if <code>graph1</code> is isomorphic to a subgraph of <code>graph2</code>. 
	 * 
	 * @return true, if <code>graph1</code> is isomorphic to a subgraph of <code>graph2</code>, false otherwise
	 */
	public boolean isSubgraphIsomorphic() {
		m_test = TEST_SUBGRAPH;
		m_mappings.clear();
		
		boolean found = match(m_state);

		createIsomorphismRelationIterator();
		
		return found;
	}
	
	/**
	 * Determines if <code>graph1</code> and <code>graph2</code> are isomorphic.
	 * 
	 * @return true, if <code>graph1</code> and <code>graph2</code> are isomorphic, false otherwise
	 */
	public boolean isIsomorphic() {
		m_test = TEST_GRAPH;
		m_mappings.clear();
		
		if (g1.vertexSet().size() != g2.vertexSet().size())
			return false;
		
		boolean found = match(m_state);
		
		createIsomorphismRelationIterator();
		
		return found;
	}
	
	private void createIsomorphismRelationIterator() {
		m_isomorphisms = new LinkedList<IsomorphismRelation<V,E>>();
		
		for (Map<V,V> map : m_mappings) {
			List<V> g1list = new LinkedList<V>();
			List<V> g2list = new LinkedList<V>();
			
			for (V v : map.keySet()) {
				g1list.add(v);
				g2list.add(map.get(v));
			}
			
			m_isomorphisms.add(new IsomorphismRelation<V,E>(g1list, g2list, g1, g2));
		}
		
		m_isomorphismIterator = m_isomorphisms.iterator();		
	}
	
	private boolean match(DiGMState state) {
		if (core1.size() == g1.vertexSet().size()) {
//			log.debug("found: " + core1);
			if (m_generateMappings) {
				m_mappings.add(new HashMap<V,V>(core1));
				if (m_listener != null) {
					List<V> g1list = new LinkedList<V>();
					List<V> g2list = new LinkedList<V>();
					
					for (V v : core1.keySet()) {
						g1list.add(v);
						g2list.add(core1.get(v));
					}
					m_listener.mapping(new IsomorphismRelation<V,E>(g1list, g2list, g1, g2));
				}
			}
			return true;
		}
		else {
			boolean found = false;
			Set<Pair> candidatePairs = getCandidatePairs();
//			log.debug("cp: " + candidatePairs);
			for (Pair p : candidatePairs) {
//				log.debug("trying " + p);
				if (isFeasible(p.n1, p.n2)) {// && isSemanticallyFeasible(p.n1, p.n2)) {
					DiGMState newState = new DiGMState(p.n1, p.n2);
					if (match(newState)) {
						found = true;
						if (!m_generateMappings)
							return true;
					}
					newState.remove();
				}
			}
			
			return found;
		}
	}
	
	private SortedSet<V> getTerminalSet(DirectedGraph<V,E> g, Map<V,V> core, Map<V,Integer> inout) {
		SortedSet<V> set = new TreeSet<V>();
		for (V n : g.vertexSet()) {
			if (inout.containsKey(n) && !core.containsKey(n))
				set.add(n);
		}
		return set;
	}
	
	private Set<Pair> getCandidatePairs() {
		Set<Pair> candidates = new HashSet<Pair>();
		
		SortedSet<V> t1out = getTerminalSet(g1, core1, out1);
		SortedSet<V> t2out = getTerminalSet(g2, core2, out2);
//		log.debug("core1: " + core1 + ", out1: " + out1 + ", t1out: " + t1out);
//		log.debug("core2: " + core2 + ", out2: " + out2 + ", t2out: " + t2out);
//		log.debug("t" + t1out + " " + t2out);
		
		if (t1out.size() != 0 && t2out.size() != 0) {
			V n1 = t1out.first();
			for (V n2 : t2out) 
				candidates.add(new Pair(n1, n2));
		}
		else {//if (t1out.size() == 0 && t2out.size() == 0) {
			SortedSet<V> t1in = getTerminalSet(g1, core1, in1);
			SortedSet<V> t2in = getTerminalSet(g2, core2, in2);
//			log.debug("t1in: " + t1in);
//			log.debug("t2in: " + t2in);
			
			if (t1in.size() != 0 && t2in.size() != 0) {
				V n1 = t1in.first();
				for (V n2 : t2in)
					candidates.add(new Pair(n1, n2));
			}
			else if (t1in.size() == 0 && t2in.size() == 0) {
				TreeSet<V> diff = new TreeSet<V>(g1.vertexSet());
				diff.removeAll(core1.keySet());
				V n1 = diff.first();
				for (V n2 : g2.vertexSet())
					if (!core2.containsKey(n2))
						candidates.add(new Pair(n1, n2));
			}
		}
		
		return candidates;
	}
	
	private boolean edgeSetsCompatible(Set<E> g1edges, Set<E> g2edges) {
		for (E e1 : g1edges) {
			boolean found = false;
			for (E e2 : g2edges) {
				if (m_checker.isEdgeCompatible(e1, e2)) {
					found = true;
					break;
				}
			}
			if (!found)
				return false;
		}
		return true;
	}
	
	private boolean edgeSetsCompatible(List<E> g1edges, List<E> g2edges) {
		for (E e1 : g1edges) {
			boolean found = false;
			for (E e2 : g2edges) {
				if (m_checker.isEdgeCompatible(e1, e2)) {
					found = true;
					break;
				}
			}
			if (!found)
				return false;
		}
		return true;
	}
	
	Map<V,Map<V,List<E>>> g1predsCache = new HashMap<V,Map<V,List<E>>>();
	Map<V,Map<V,List<E>>> g2predsCache = new HashMap<V,Map<V,List<E>>>();
	
	private Map<V,List<E>> preds(DirectedGraph<V,E> g, V v) {
		Map<V,Map<V,List<E>>> predsCache;
		if (g == g1)
			predsCache = g1predsCache;
		else
			predsCache = g2predsCache;
		
		Map<V,List<E>> preds = predsCache.get(v);
		if (preds == null) {
			preds = new HashMap<V,List<E>>();
			for (E inEdge : g.incomingEdgesOf(v)) {
				V pred = g.getEdgeSource(inEdge);
				List<E> edges = preds.get(pred);
				if (edges == null) {
					edges = new ArrayList<E>();
					preds.put(pred, edges);
				}
				edges.add(inEdge);
			}
			predsCache.put(v, preds);
		}

		return preds;
	}
	
	Map<V,Map<V,List<E>>> g1succsCache = new HashMap<V,Map<V,List<E>>>();
	Map<V,Map<V,List<E>>> g2succsCache = new HashMap<V,Map<V,List<E>>>();
	
	private Map<V,List<E>> succs(DirectedGraph<V,E> g, V v) {
		Map<V,Map<V,List<E>>> succsCache;
		if (g == g1)
			succsCache = g1succsCache;
		else
			succsCache = g2succsCache;
		
		Map<V,List<E>> succs = succsCache.get(v);
		if (succs == null) {
			succs = new HashMap<V,List<E>>();
			for (E outEdge : g.outgoingEdgesOf(v)) {
				V succ = g.getEdgeTarget(outEdge);
				List<E> edges = succs.get(succ);
				if (edges == null) {
					edges = new ArrayList<E>();
					succs.put(succ, edges);
				}
				edges.add(outEdge);
			}
			succsCache.put(v, succs);
		}

		return succs;
	}
	
	private boolean isFeasibleIso(V n1, V n2) {
		// R_self
//		if (g1.getAllEdges(n1, n1).size() != g2.getAllEdges(n2, n2).size())
//			return false;
		
		// R_pred
		Map<V,List<E>> n1preds = preds(g1, n1);
//		for (V n1pred : predecessors(g1, n1)) {
		for (V n1pred : n1preds.keySet()) {
			if (core1.containsKey(n1pred)) {
//				Set<E> g1edges = g1.getAllEdges(n1pred, n1);
				List<E> g1edges = n1preds.get(n1pred);
//				Set<V> n2preds = predecessors(g2, n2);
				Map<V,List<E>> n2preds = preds(g2, n2);
				
				if (g1edges.size() > 0) {
//					for (V n2pred : n2preds) {
					for (V n2pred : n2preds.keySet()) {
						if (n2pred.equals(core1.get(n1pred))) {
//							Set<E> g2edges = g2.getAllEdges(core1.get(n1pred), n2);
							List<E> g2edges = n2preds.get(n2pred);
							if (g1edges.size() > g2edges.size())
								return false;
							if (!edgeSetsCompatible(g1edges, g2edges))
								return false;
						}
					}
				}
				else {
					log.error("should not happen, g1edges should be > 0");
				}
//				if (!n2preds.contains(core1.get(n1pred))) 
//					return false;
//				else if (g1.getAllEdges(n1pred, n1).size() > g2.getAllEdges(core1.get(n1pred), n2).size())
//					return false;
			}
		}
		
//		for (V n2pred : predecessors(g2, n2)) {
//			if (core2.containsKey(n2pred)) {
//				Set<V> n1preds = predecessors(g1, n1); 
//				if (!n1preds.contains(core2.get(n2pred)))
//					return false;
//				else if (g2.getAllEdges(n2pred, n2).size() != g1.getAllEdges(core2.get(n2pred), n1).size())
//					return false;
//			}
//		}
		
		// R_succ
		for (V n1succ : successors(g1, n1)) {
			if (core1.containsKey(n1succ)) {
				Set<E> g1edges = g1.getAllEdges(n1, n1succ);
				Set<V> n2succs = successors(g2, n2);
				
				if (g1edges.size() > 0) {
					for (V n2succ : n2succs) {
						if (n2succ.equals(core1.get(n1succ))) {
							Set<E> g2edges = g2.getAllEdges(n2, core1.get(n1succ));
							if (g1edges.size() > g2edges.size())
								return false;
							if (!edgeSetsCompatible(g1edges, g2edges))
								return false;
						}
					}
					
					if (!n2succs.contains(core1.get(n1succ)))
						log.debug("22");
					else
						log.debug("11");

				}
				else {
					log.error("should not happen, g1edges should be > 0");
				}
//				if (!n2succs.contains(core1.get(n1succ)))
//					return false;
//				else if (g1.getAllEdges(n1succ, n1).size() > g2.getAllEdges(core1.get(n1succ), n2).size())
//					return false;
			}
		}
		
//		for (V n2succ : successors(g2, n2)) {
//			if (core2.containsKey(n2succ)) {
//				Set<V> n1succs = successors(g1, n1); 
//				if (!n1succs.contains(core2.get(n2succ)))
//					return false;
//				else if (g2.getAllEdges(n2succ, n2).size() != g1.getAllEdges(core2.get(n2succ), n1).size())
//					return false;
//			}
//		}
		
		// R_termin
		int num1 = count(predecessors(g1, n1), in1.keySet(), core1.keySet());
		int num2 = count(predecessors(g2, n2), in2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;
		
		num1 = count(successors(g1, n1), in1.keySet(), core1.keySet());
		num2 = count(successors(g2, n2), in2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;

		// R_termout
		num1 = count(predecessors(g1, n1), out1.keySet(), core1.keySet());
		num2 = count(predecessors(g2, n2), out2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;
		
		num1 = count(successors(g1, n1), out1.keySet(), core1.keySet());
		num2 = count(successors(g2, n2), out2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;
		
		// R_new
		num1 = count(predecessors(g1, n1), in1.keySet(), out1.keySet());
		num2 = count(predecessors(g2, n2), in2.keySet(), out2.keySet());
		if (!compareCount(num1, num2))
			return false;
		
		num1 = count(successors(g1, n1), in1.keySet(), out1.keySet());
		num2 = count(successors(g2, n2), in2.keySet(), out2.keySet());
		if (!compareCount(num1, num2))
			return false;
		
		return true;
	}
	
	
	private boolean isFeasibleSubgraph(V n1, V n2) {
//		log.debug(" n1: " + n1 + ", n2: " + n2);
//		log.debug(" core1: " + core1);
		int termin1 = 0, termin2 = 0, termout1 = 0, termout2 = 0, new1 = 0, new2 = 0; 
		// R_pred
		Map<V,List<E>> n1predMap = preds(g1, n1);
		Map<V,List<E>> n2predMap = preds(g2, n2);
//		for (V n1pred : predecessors(g1, n1)) {
		for (V n1pred : n1predMap.keySet()) {
			V n1mapped = core1.get(n1pred);
			if (n1mapped != null) {
				List<E> n1predEdges = n1predMap.get(n1pred);
//				Set<V> n2preds = predecessors(g2, n2);
//				if (!n2preds.contains(core1.get(n1pred)))
				if (!n2predMap.keySet().contains(n1mapped))
					return false;
//				else if (g1.getAllEdges(n1pred, n1).size() > g2.getAllEdges(core1.get(n1pred), n2).size())
				else if (n1predEdges.size() > n2predMap.get(n1mapped).size())
					return false;

//				Set<E> g1edges = g1.getAllEdges(n1pred, n1);
				List<E> g1edges = n1predEdges;
				
				boolean found = false;
				for (V n2pred : n2predMap.keySet()) {
					if (core2.containsKey(n2pred)) {
//						Set<E> g2edges = g2.getAllEdges(n2pred, n2);
						List<E> g2edges = n2predMap.get(n2pred);
						if (edgeSetsCompatible(g1edges, g2edges)) {
							found = true;
							break;
						}
					}
				}
				if (!found)
					return false;
			}
			else {
				if (in1.containsKey(n1pred))
					termin1++;
				if (out1.containsKey(n1pred))
					termout1++;
				if (!in1.containsKey(n1pred) && !out1.containsKey(n1pred))
					new1++;
			}
		}
		
//		for (V n2pred : predecessors(g2, n2)) {
		for (V n2pred : n2predMap.keySet()) {
			if (core2.containsKey(n2pred)) {
				// monomorphism: do nothing
			}
			else {
				if (in2.containsKey(n2pred))
					termin2++;
				if (out2.containsKey(n2pred))
					termout2++;
				if (!in2.containsKey(n2pred) && !out2.containsKey(n2pred))
					new2++;
			}
		}
		
		// R_succ
		Map<V,List<E>> n1succs = succs(g1, n1);
		Map<V,List<E>> n2succs = succs(g2, n2);
//		for (V n1succ : successors(g1, n1)) {
		for (V n1succ : n1succs.keySet()) {
			V n1mapped = core1.get(n1succ);
			if (n1mapped != null) {
				List<E> n1succEdges = n1succs.get(n1succ);
//				Set<V> n2succs = successors(g2, n2);
//				if (!n2succs.contains(core1.get(n1succ)))
				if (!n2succs.keySet().contains(n1mapped))
					return false;
//				else if (g1.getAllEdges(n1succ, n1).size() > g2.getAllEdges(core1.get(n1succ), n2).size())
				else if (n1succEdges.size() > n2succs.get(n1mapped).size())
					return false;
				
//				Set<E> g1edges = g1.getAllEdges(n1, n1succ);
				List<E> g1edges = n1succEdges;
				
				boolean found = false;
//				for (V n2succ : n2succs) {
				for (V n2succ : n2succs.keySet()) {
					if (core2.containsKey(n2succ)) {
//						Set<E> g2edges = g2.getAllEdges(n2, n2succ);
						List<E> g2edges = n2succs.get(n2succ);
						if (edgeSetsCompatible(g1edges, g2edges)) {
							found = true;
							break;
						}
					}
				}
				if (!found)
					return false;
			}
			else {
				if (in1.containsKey(n1succ))
					termin1++;
				if (out1.containsKey(n1succ))
					termout1++;
				if (!in1.containsKey(n1succ) && !out1.containsKey(n1succ))
					new1++;
				
			}
		}
		
//		for (V n2succ : successors(g2, n2)) {
		for (V n2succ : n2succs.keySet()) {
			if (core2.containsKey(n2succ)) {
				// monomorphism: do nothing
			}
			else {
				if (in2.containsKey(n2succ))
					termin2++;
				if (out2.containsKey(n2succ))
					termout2++;
				if (!in2.containsKey(n2succ) && !out2.containsKey(n2succ))
					new2++;
			}
		}
		
		return termin1 <= termin2 && termout1 <= termout2 && (termin1 + termout1 + new1) <= (termin2 + termout2 + new2);
	}

	private boolean isFeasible(V n1, V n2) {
		if (!m_checker.isVertexCompatible(n1, n2))
			return false;
		
		if (m_test == TEST_GRAPH)
			return isFeasibleIso(n1, n2);
		else
			return isFeasibleSubgraph(n1, n2);
	}
	
	private boolean compareCount(int num1, int num2) {
		if (m_test == TEST_GRAPH) {
			if (num1 != num2)
				return false;
		}
		else if (num1 < num2)
			return false;
		return true;
	}
	
	private int count(Set<V> nodes, Set<V> inout, Set<V> core) {
		int n = 0;
		for (V node : nodes) {
			if (inout.contains(node) && !core.contains(node))
				n++;
		}
		return n;
	}

	/**
	 * The number of mappings generated by the previous call to isSubgraphIsomorphic() or 
	 * isIsomorphic(). If the matcher was created with <code>generateMappings=false</code> this
	 * will always return zero.
	 * 
	 * @return the number of mappings
	 */
	public int numberOfMappings() {
		return m_mappings.size();
	}

	public boolean hasNext() {
		return m_isomorphismIterator.hasNext();
	}

	public IsomorphismRelation<V,E> next() {
		return m_isomorphismIterator.next();
	}
	
	public void remove() {
		throw new UnsupportedOperationException("removing not supported");
	}

	public Iterator<IsomorphismRelation<V,E>> iterator() {
		return this;
	}
}
