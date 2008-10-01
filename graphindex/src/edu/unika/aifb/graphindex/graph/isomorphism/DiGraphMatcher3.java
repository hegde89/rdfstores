package edu.unika.aifb.graphindex.graph.isomorphism;

import java.util.ArrayList;
import java.util.Collection;
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
import org.jgrapht.graph.ClassBasedEdgeFactory;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * Implementation of the VF2 graph isomorphism algorithm, published in:
 * <p>
 * Luigi P. Cordella, Pasquale Foggia, Carlo Sansone, Mario Vento,
 * "A (Sub)Graph Isomorphism Algorithm for Matching Large Graphs," IEEE
 * Transactions on Pattern Analysis and Machine Intelligence, vol. 26, no. 10,
 * pp. 1367-1372, Oct., 2004.
 * <p>
 * Inspired by the implementation in the NetworkX graph library for Python:
 * {@link https://networkx.lanl.gov/wiki}
 * 
 * @author gl
 * 
 * @param <String>
 * @param <LabeledEdge<String>>
 */
public class DiGraphMatcher3 implements Iterable<IsomorphismRelation<String,LabeledEdge<String>>> {

	public class DiGMState {
		private int n1, n2;
		private int core_len, orig_core_len;
		private int added_node1;
		private int t1both_len, t2both_len, t1in_len, t1out_len, t2in_len, t2out_len;

		public DiGMState() {
			n1 = g1.nodeCount();
			n2 = g2.nodeCount();
			
			core_len = orig_core_len = 0;
			t1both_len = t1in_len = t1out_len = 0;
			t2both_len = t2in_len = t2out_len = 0;
			
			added_node1 = NULL_NODE;

			for (int i = 0; i < g1.nodeCount(); i++) {
				core1[i] = NULL_NODE;
				in1[i] = 0;
				out1[i] = 0;
			}

			for (int i = 0; i < g2.nodeCount(); i++) {
				core2[i] = NULL_NODE;
				in2[i] = 0;
				out2[i] = 0;
			}
			
		}
		
		public DiGMState(DiGMState state) {

			n1 = state.n1;
			n2 = state.n2;

			core_len = orig_core_len = state.core_len;
			
			t1in_len = state.t1in_len;
			t1out_len = state.t1out_len;
			t1both_len = state.t1both_len;
			
			t2in_len = state.t2in_len;
			t2out_len = state.t2out_len;
			t2both_len = state.t2both_len;
			
			added_node1 = NULL_NODE;
		}
		
		public int coreLength() {
			return core_len;
		}
		
		public Map<String,String> getMapping() {
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < n1; i++) {
				if (core1[i] != NULL_NODE) {
					map.put(g1.getNodeLabel(i), g2.getNodeLabel(core1[i]));
				}
			}
			
			return map;
		}
		
		public Pair nextPair(int prev_n1, int prev_n2) {
			do {
				if (prev_n1 == NULL_NODE)
					prev_n1 = 0;
				if (prev_n2 == NULL_NODE)
					prev_n2 = 0;
				else
					prev_n2++;
			
				if (t1both_len > core_len && t2both_len > core_len) {
					while (prev_n1 < n1 && (core1[prev_n1] != NULL_NODE || out1[prev_n1] == 0 || in1[prev_n1] == 0)) {
						prev_n1++;
						prev_n2 = 0;
					}
				}
				else if (t1out_len > core_len && t2out_len > core_len) {
					while (prev_n1 < n1 && (core1[prev_n1] != NULL_NODE || out1[prev_n1] == 0)) {
						prev_n1++;
						prev_n2 = 0;
					}
				}
				else if (t1in_len > core_len && t2in_len > core_len) {
					while (prev_n1 < n1 && (core1[prev_n1] != NULL_NODE || in1[prev_n1] == 0)) {
						prev_n1++;
						prev_n2 = 0;
					}
				}
				else {
					while (prev_n1 < n1 && core1[prev_n1] != NULL_NODE) {
						prev_n1++;
						prev_n2 = 0;
					}
				}
				
				if (t1both_len > core_len && t2both_len > core_len) { 
					while (prev_n2 < n2 && (core2[prev_n2] != NULL_NODE || out2[prev_n2] == 0 || in2[prev_n2] == 0)) { 
						prev_n2++;    
					}
				}
				else if (t1out_len > core_len && t2out_len > core_len) { 
					while (prev_n2 < n2 && (core2[prev_n2] != NULL_NODE || out2[prev_n2] == 0)) { 
						prev_n2++;    
					}
				}
			    else if (t1in_len > core_len && t2in_len > core_len) { 
			    	while (prev_n2 < n2 && (core2[prev_n2] != NULL_NODE || in2[prev_n2] == 0)) { 
			    		prev_n2++;    
			    	}
			    }
				else { 
					while (prev_n2 < n2 && core2[prev_n2] != NULL_NODE) {
						prev_n2++;    
					}
				}
			}
			while (prev_n1 < n1 && prev_n2 < n2 && !m_checker.isVertexCompatible(g1.getNodeLabel(prev_n1), g2.getNodeLabel(prev_n2)));
			
			if (prev_n1 < n1 && prev_n2 < n2) {
				return new Pair(prev_n1, prev_n2);
			}
			
			return null;
		}
		
		public void addPair(int node1, int node2) {
			core_len++;
			added_node1 = node1;
			
			if (in1[node1] == 0) {
				in1[node1] = core_len;
				t1in_len++;
				if (out1[node1] != 0)
					t1both_len++;
			}
			
			if (out1[node1] == 0) {
				out1[node1] = core_len;
				t1out_len++;
				if (in1[node1] != 0)
					t1both_len++;
			}

			if (in2[node2] == 0) {
				in2[node2] = core_len;
				t2in_len++;
				if (out2[node2] != 0)
					t2both_len++;
			}
			
			if (out2[node2] == 0) {
				out2[node2] = core_len;
				t2out_len++;
				if (in2[node2] != 0)
					t2both_len++;
			}
			
			core1[node1] = node2;
			core2[node2] = node1;
			
			for (int pred : g1.predecessors(node1, m_labels)) {
				if (in1[pred] == 0) {
					in1[pred] = core_len;
					t1in_len++;
					if (out1[pred] != 0)
						t1both_len++;
				}
			}

			for (int succ : g1.successors(node1, m_labels)) {
				if (out1[succ] == 0) {
					out1[succ] = core_len;
					t1out_len++;
					if (in1[succ] != 0)
						t1both_len++;
				}
			}

			for (int pred : g2.predecessors(node2, m_labels)) {
				if (in2[pred] == 0) {
					in2[pred] = core_len;
					t2in_len++;
					if (out2[pred] != 0)
						t2both_len++;
				}
			}

			for (int succ : g2.successors(node2, m_labels)) {
				if (out2[succ] == 0) {
					out2[succ] = core_len;
					t2out_len++;
					if (in2[succ] != 0)
						t2both_len++;
				}
			}
		}

		public void remove() {
			if (core_len - orig_core_len > 1 || added_node1 == NULL_NODE)
				throw new Error("error");
			
//			log.debug(" core_len: " + core_len + ", added_node1: " + added_node1);
			
			if (orig_core_len < core_len) { // check that the state wasn't already removed
				if (in1[added_node1] == core_len) 
					in1[added_node1] = 0;
				for (int pred : g1.predecessors(added_node1, m_labels)) {
//					log.debug(" pred1: " + pred);
					if (in1[pred] == core_len)
						in1[pred] = 0;
				}

				if (out1[added_node1] == core_len)
					out1[added_node1] = 0;
				for (int succ : g1.successors(added_node1, m_labels)) {
//					log.debug(" succ1: " + succ);
					if (out1[succ] == core_len)
						out1[succ] = 0;
				}
				
				int node2 = core1[added_node1];
				
				if (in2[node2] == core_len) 
					in2[node2] = 0;
				for (int pred : g2.predecessors(node2, m_labels)) {
//					log.debug(" pred2: " + pred);
					if (in2[pred] == core_len)
						in2[pred] = 0;
				}

				if (out2[node2] == core_len) 
					out2[node2] = 0;
				for (int succ : g2.successors(node2, m_labels)) {
//					log.debug( " succ2: " + succ);
					if (out2[succ] == core_len)
						out2[succ] = 0;
				}

				core1[added_node1] = NULL_NODE;
				core2[node2] = NULL_NODE;
				
				core_len = orig_core_len;
				added_node1 = NULL_NODE;
			}
		}
	}

	private class Pair {
		public int n1, n2;

		public Pair(int n1, int n2) {
			this.n1 = n1;
			this.n2 = n2;
		}

		public String toString() {
			return "(" + n1 + "," + n2 + ") (" + g1.getNodeLabel(n1) + "," + g2.getNodeLabel(n2) + ")";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + n1;
			result = prime * result + n2;
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
			if (n1 != other.n1)
				return false;
			if (n2 != other.n2)
				return false;
			return true;
		}
	}

	private final int NULL_NODE = -1;
	
	private IndexGraph g1, g2;
	private Set<String> m_labels;
	
	private int[] core1, core2;
	private int[] in1, in2, out1, out2;
	
	
	private DiGMState m_state;
	
	private List<IsomorphismRelation<String,LabeledEdge<String>>> m_isomorphisms;

	private FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> m_checker;
	private MappingListener<String,LabeledEdge<String>> m_listener;
	
	private boolean m_generateMappings;
	
	private final static Logger log = Logger.getLogger(DiGraphMatcher3.class);

	/**
	 * Create a new matcher object. If testing for subgraph isomorphism,
	 * <code>graph1</code> should be the "smaller" graph. If
	 * <code>generateMappings</code> is true, all possible mappings are
	 * generated, which can take time. Otherwise, the algorithm stops as early
	 * as possible and does not generate any mappings.
	 * <p>
	 * This class also implements the Iterable interface, which is used to
	 * iterate over all mappings generated.
	 * 
	 * @param graph1
	 * @param graph2
	 * @param generateMappings
	 *            whether to generate mappings or just check for isomorphisms
	 */
	public DiGraphMatcher3(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings) {
		g1 = new IndexGraph(graph1);
		g2 = new IndexGraph(graph2);
		m_labels = new HashSet<String>();
		for (LabeledEdge<String> e : graph1.edgeSet())
			m_labels.add(e.getLabel());

		core1 = new int[g1.nodeCount()];
		in1 = new int[g1.nodeCount()];
		out1 = new int[g1.nodeCount()];
		core2 = new int[g2.nodeCount()];
		in2 = new int[g2.nodeCount()];
		out2 = new int[g2.nodeCount()];
		
		m_state = new DiGMState();

		m_isomorphisms = new ArrayList<IsomorphismRelation<String,LabeledEdge<String>>>();
		// default checker implementation, always true
		m_checker = new FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>>() { 
			public boolean isEdgeCompatible(LabeledEdge<String> e1, LabeledEdge<String> e2) {
				return true;
			}

			public boolean isVertexCompatible(String n1, String n2) {
				return true;
			}

			public boolean checkVertexCompatible(String n1, String n2) {
				return true;
			}
		};
		m_listener = null;
		m_generateMappings = generateMappings;
	}

	/**
	 * Create a new matcher object. If testing for subgraph isomorphism,
	 * <code>graph1</code> should be the "smaller" graph. If
	 * <code>generateMappings</code> is true, all possible mappings are
	 * generated, which can take time. Otherwise, the algorithm stops as early
	 * as possible and does not generate any mappings. The feasibility checker
	 * is used to determine semantic feasibility.
	 * 
	 * @param graph1
	 * @param graph2
	 * @param generateMappings
	 * @param checker
	 *            a FeasibilityChecker which is used to determine if the current
	 *            mapping can be extended by the two nodes
	 */
	public DiGraphMatcher3(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings,
			FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> checker) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
	}

	public DiGraphMatcher3(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings,
			FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> checker,
			MappingListener<String,LabeledEdge<String>> listener) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
		m_listener = listener;
	}

	public DiGraphMatcher3(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings,
			MappingListener<String,LabeledEdge<String>> listener) {
		this(graph1, graph2, generateMappings);
		m_listener = listener;
	}

	/**
	 * Determines if <code>graph1</code> is isomorphic to a subgraph of
	 * <code>graph2</code>.
	 * 
	 * @return true, if <code>graph1</code> is isomorphic to a subgraph of
	 *         <code>graph2</code>, false otherwise
	 */
	public boolean isSubgraphIsomorphic() {
		m_isomorphisms.clear();

		boolean found = match(m_state);

		return found;
	}

	private IsomorphismRelation<String,LabeledEdge<String>> createIsomorphismRelation(Map<String,String> map) {
		List<String> g1list = new LinkedList<String>();
		List<String> g2list = new LinkedList<String>();

		for (String v : map.keySet()) {
			g1list.add(v);
			g2list.add(map.get(v));
		}

		return new IsomorphismRelation<String,LabeledEdge<String>>(g1list, g2list, g1.getNamedGraph(), g2.getNamedGraph());
	}

	static int mid = 0;
	public int pairs = 0;
	private boolean match(DiGMState state) {
		if (state.coreLength() == g1.nodeCount()) {
			boolean valid = true;
			Map<String,String> mapping = m_state.getMapping();
//			for (int n1 : core1)
//				System.out.print(core2[n1] + "=" + n1 + " ");
//			System.out.println();
			for (String n1 : mapping.keySet())
				valid = m_checker.checkVertexCompatible(n1, mapping.get(n1)) && valid; // lazy evaluation
			if (valid) {
				log.debug("found: " + mapping);
				if (m_generateMappings) {
					IsomorphismRelation<String,LabeledEdge<String>> iso = createIsomorphismRelation(mapping);
					m_isomorphisms.add(iso);
					if (m_listener != null)
						m_listener.mapping(iso);
				}
				return true;
			}
			return false;
		} else {
			boolean found = false;
			Pair p = new Pair(NULL_NODE, NULL_NODE);
			while ((p = m_state.nextPair(p.n1, p.n2)) != null) {
				pairs++;
//				log.debug("trying " + p + ", current mapping: " + state.getMapping());
				if (isFeasible(p.n1, p.n2, state)) {
					DiGMState newState = new DiGMState(state);
					newState.addPair(p.n1, p.n2);
					if (match(newState)) {
						found = true;
						if (!m_generateMappings)
							return true;
					}
					
//					log.debug("before: " + atos(in1) + " " + atos(in2) + " " + atos(out1) + " " + atos(out2));
					newState.remove();
//					log.debug("after:  " + atos(in1) + " " + atos(in2) + " " + atos(out1) + " " + atos(out2));
					
					if (isInvalidMapping(state.getMapping()))
						return false;
				}
			}

			return found;
		}
	}
	
	private boolean isInvalidMapping(Map<String,String> map) {
		for (String n1 : map.keySet())
			if (!m_checker.isVertexCompatible(n1, map.get(n1)))
				return true;
		return false;
	}

	private boolean edgeSetsCompatible(Set<LabeledEdge<String>> g1edges, Set<LabeledEdge<String>> g2edges) {
		for (LabeledEdge<String> e1 : g1edges) {
			boolean found = false;
			for (LabeledEdge<String> e2 : g2edges) {
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

	private class Tuple {
		List<IndexEdge> l1, l2;

		public Tuple(List<IndexEdge> l1, List<IndexEdge> l2) {
			this.l1 = l1;
			this.l2 = l2;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((l1 == null) ? 0 : l1.hashCode());
			result = prime * result + ((l2 == null) ? 0 : l2.hashCode());
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
			Tuple other = (Tuple)obj;
			if (l1 == null) {
				if (other.l1 != null)
					return false;
			} else if (!l1.equals(other.l1))
				return false;
			if (l2 == null) {
				if (other.l2 != null)
					return false;
			} else if (!l2.equals(other.l2))
				return false;
			return true;
		}
	}

	Map<Tuple,Boolean> edgeSetCache = new HashMap<Tuple,Boolean>();

	private boolean edgeSetsCompatible(List<IndexEdge> g1edges, List<IndexEdge> g2edges) {
		Tuple t = new Tuple(g1edges, g2edges);
		Boolean val = edgeSetCache.get(t);
		if (val == null) {
			for (IndexEdge e1 : g1edges) {
				boolean found = false;
				for (IndexEdge e2 : g2edges) {
					if (e1.getLabel().equals(e2.getLabel())) {
						found = true;
						break;
					}
				}
				if (!found) {
					val = Boolean.FALSE;
					break;
				}
			}
			if (val == null)
				val = Boolean.TRUE;
			edgeSetCache.put(t, val);
		}
		return val;
	}
	
	private boolean isFeasibleSubgraph(int n1, int n2) {
		int termin1 = 0, termin2 = 0, termout1 = 0, termout2 = 0, new1 = 0, new2 = 0;
		
		if (g2.outDegreeOf(n2) < g1.outDegreeOf(n1) || g2.inDegreeOf(n2) < g1.inDegreeOf(n1))
			return false;
		
		if (!g2.outEdgeLabels(n2).containsAll(g1.outEdgeLabels(n1))) {
			return false;
		}
		
		if (!g2.inEdgeLabels(n2).containsAll(g1.inEdgeLabels(n1))) {
			return false;
		}
		
		// R_pred
		Map<Integer,List<IndexEdge>> n1predMap = g1.predecessorEdges(n1);
		Map<Integer,List<IndexEdge>> n2predMap = g2.predecessorEdges(n2);
		for (int n1pred : n1predMap.keySet()) {
			int n1mapped = core1[n1pred];
			if (n1mapped != NULL_NODE) {
				List<IndexEdge> n1predEdges = n1predMap.get(n1pred);

				if (!n2predMap.keySet().contains(n1mapped))
					return false;
				else if (n1predEdges.size() > n2predMap.get(n1mapped).size())
					return false;

				List<IndexEdge> g1edges = n1predEdges;

				boolean found = false;
				for (int n2pred : n2predMap.keySet()) {
					if (core2[n2pred] != NULL_NODE) {
						List<IndexEdge> g2edges = n2predMap.get(n2pred);
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
				if (in1[n1pred] != 0)
					termin1++;
				if (out1[n1pred] != 0)
					termout1++;
				if (in1[n1pred] == 0 && out1[n1pred] == 0)
					new1++;
			}
		}

		for (int n2pred : n2predMap.keySet()) {
			if (core2[n2pred] != NULL_NODE) {
				// monomorphism: do nothing
			} else {
				if (in2[n2pred] != 0)
					termin2++;
				if (out2[n2pred] != 0)
					termout2++;
				if (in2[n2pred] == 0 && out2[n2pred] == 0)
					new2++;
			}
		}

		// R_succ
		Map<Integer,List<IndexEdge>> n1succs = g1.successorEdges(n1);
		Map<Integer,List<IndexEdge>> n2succs = g2.successorEdges(n2);
		for (int n1succ : n1succs.keySet()) {
			int n1mapped = core1[n1succ];
			if (n1mapped != NULL_NODE) {
				List<IndexEdge> n1succEdges = n1succs.get(n1succ);

				if (!n2succs.keySet().contains(n1mapped))
					return false;
				else if (n1succEdges.size() > n2succs.get(n1mapped).size())
					return false;

				List<IndexEdge> g1edges = n1succEdges;

				boolean found = false;
				for (int n2succ : n2succs.keySet()) {
					if (core2[n2succ] != NULL_NODE) {
						List<IndexEdge> g2edges = n2succs.get(n2succ);
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
				if (in1[n1succ] != 0)
					termin1++;
				if (out1[n1succ] != 0)
					termout1++;
				if (in1[n1succ] == 0 && out1[n1succ] == 0)
					new1++;

			}
		}

		for (int n2succ : n2succs.keySet()) {
			if (core2[n2succ] != NULL_NODE) {
				// monomorphism: do nothing
			} else {
				if (in2[n2succ] != 0)
					termin2++;
				if (out2[n2succ] != 0)
					termout2++;
				if (in2[n2succ] == 0 && out2[n2succ] == 0)
					new2++;
			}
		}

//		log.debug(atos(in1) + " " + atos(in2) + " " + atos(out1) + " " + atos(out2));
//		log.debug(termin1 + " " + termin2 + " " + termout1 + " " + termout2 + " " + new1 + " " + new2);
		
		return termin1 <= termin2 && termout1 <= termout2
				&& (termin1 + termout1 + new1) <= (termin2 + termout2 + new2);
	}
	
	private String atos(int[] a) {
		String s = "{";
		String comma = "";
		for (int i = 0; i < a.length; i++) {
//			s += comma + i + "=" + a[i];
			s += comma + a[i];
			comma = ",";
		}
		return s + "}";
	}

	private boolean isFeasible(int n1, int n2, DiGMState state) {
		
		if (state.coreLength() == 0) {
			if (!m_checker.isVertexCompatible(g1.getNodeLabel(n1), g2.getNodeLabel(n2)))
				return false;
		}
		else {
			if (!m_checker.checkVertexCompatible(g1.getNodeLabel(n1), g2.getNodeLabel(n2)))
				return false;
		}

		return isFeasibleSubgraph(n1, n2);
	}

	/**
	 * The number of mappings generated by the previous call to
	 * isSubgraphIsomorphic() or isIsomorphic(). If the matcher was created with
	 * <code>generateMappings=false</code> this will always return zero.
	 * 
	 * @return the number of mappings
	 */
	public int numberOfMappings() {
		return m_isomorphisms.size();
	}

	public Iterator<IsomorphismRelation<String,LabeledEdge<String>>> iterator() {
		return m_isomorphisms.iterator();
	}
}
