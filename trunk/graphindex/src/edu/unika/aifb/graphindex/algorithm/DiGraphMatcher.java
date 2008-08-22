package edu.unika.aifb.graphindex.algorithm;

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

import edu.unika.aifb.graphindex.test.FeasibilityChecker;

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
	private int m_test;
	private boolean m_generateMappings;
	private final static Logger log = Logger.getLogger(DiGraphMatcher.class);
	
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings) {
		g1 = graph2;
		g2 = graph1;
		
		core1 = new HashMap<V,V>();
		core2 = new HashMap<V,V>();
		in1 = new HashMap<V,Integer>();
		in2 = new HashMap<V,Integer>();
		out1 = new HashMap<V,Integer>();
		out2 = new HashMap<V,Integer>();
		
		m_state = new DiGMState(null, null);
		
		m_mappings = new ArrayList<Map<V,V>>();
		m_test = TEST_GRAPH;
		m_checker = null;
		m_generateMappings = generateMappings;
	}
	
	public DiGraphMatcher(DirectedGraph<V,E> graph1, DirectedGraph<V,E> graph2, boolean generateMappings, FeasibilityChecker<V,E,DirectedGraph<V,E>> checker) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
	}
	
	private Set<V> predecessors(DirectedGraph<V,E> g, V v) {
		Set<V> preds = new HashSet<V>();
		for (E edge : g.incomingEdgesOf(v)) {
			preds.add(g.getEdgeSource(edge));
		}
		return preds;
	}
	
	private Set<V> successors(DirectedGraph<V,E> g, V v) {
		Set<V> succs = new HashSet<V>();
		for (E edge : g.outgoingEdgesOf(v)) {
			succs.add(g.getEdgeTarget(edge));
		}
		return succs;
	}
	
	public boolean isSubgraphIsomorphic() {
		m_test = TEST_SUBGRAPH;
		m_mappings.clear();
		
		boolean found = match(m_state);

		createIsomorphismRelationIterator();
		
		return found;
	}
	
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
		if (core1.size() == g2.vertexSet().size()) {
			if (m_generateMappings)
				m_mappings.add(new HashMap<V,V>(core1));
			return true;
//			log.debug("nm:" + core1);
		}
		else {
			boolean found = false;
			Set<Pair> candidatePairs = getCandidatePairs();
//			log.debug("cp: " + candidatePairs);
			for (Pair p : candidatePairs) {
//				log.debug("trying " + p);
				if (isSyntacticallyFeasible(p.n1, p.n2) && isSemanticallyFeasible(p.n1, p.n2)) {
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
//		log.debug("core1: " + core1 + ", out1: " + out1);
//		log.debug("core2: " + core2 + ", out2: " + out2);
//		log.debug("t" + t1out + " " + t2out);
		
		if (t1out.size() != 0 && t2out.size() != 0) {
			V n2 = t2out.first();
			for (V n1 : t1out) 
				candidates.add(new Pair(n1, n2));
		}
		else {//if (t1out.size() == 0 && t2out.size() == 0) {
			SortedSet<V> t1in = getTerminalSet(g1, core1, in1);
			SortedSet<V> t2in = getTerminalSet(g2, core2, in2);
//			log.debug("t1in: " + t1in);
//			log.debug("t2in: " + t2in);
			
			if (t1in.size() != 0 && t2in.size() != 0) {
				V n2 = t2in.first();
				for (V n1 : t1in)
					candidates.add(new Pair(n1, n2));
			}
			else if (t1in.size() == 0 && t2in.size() == 0) {
				TreeSet<V> diff = new TreeSet<V>(g2.vertexSet());
				diff.removeAll(core2.keySet());
				V n2 = diff.first();
				for (V n1 : g1.vertexSet())
					if (!core1.containsKey(n1))
						candidates.add(new Pair(n1, n2));
			}
		}
		
		return candidates;
	}
	
	private boolean isSemanticallyFeasible(V n1, V n2) {
		return m_checker != null ? m_checker.isSemanticallyFeasible(g1, g2, n1, n2, core1, core2) : true;
	}

	private boolean isSyntacticallyFeasible(V n1, V n2) {
		
		// R_self
//		if (g1.getAllEdges(n1, n1).size() != g2.getAllEdges(n2, n2).size())
//			return false;
		
		// R_pred
		for (V n1pred : predecessors(g1, n1)) {
			if (core1.containsKey(n1pred)) {
				Set<V> n2preds = predecessors(g2, n2); 
				if (!n2preds.contains(core1.get(n1pred)))
					return false;
				else if (g1.getAllEdges(n1pred, n1).size() != g2.getAllEdges(core1.get(n1pred), n2).size())
					return false;
			}
		}
		
		for (V n2pred : predecessors(g2, n2)) {
			if (core2.containsKey(n2pred)) {
				Set<V> n1preds = predecessors(g1, n1); 
				if (!n1preds.contains(core2.get(n2pred)))
					return false;
				else if (g2.getAllEdges(n2pred, n2).size() != g1.getAllEdges(core2.get(n2pred), n1).size())
					return false;
			}
		}
		
		// R_succ
		for (V n1succ : successors(g1, n1)) {
			if (core1.containsKey(n1succ)) {
				Set<V> n2succs = successors(g2, n2); 
				if (!n2succs.contains(core1.get(n1succ)))
					return false;
				else if (g1.getAllEdges(n1succ, n1).size() != g2.getAllEdges(core1.get(n1succ), n2).size())
					return false;
			}
		}
		
		for (V n2succ : successors(g2, n2)) {
			if (core2.containsKey(n2succ)) {
				Set<V> n1succs = successors(g1, n1); 
				if (!n1succs.contains(core2.get(n2succ)))
					return false;
				else if (g2.getAllEdges(n2succ, n2).size() != g1.getAllEdges(core2.get(n2succ), n1).size())
					return false;
			}
		}
		
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
