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
public class DiGraphMatcher2 implements Iterable<IsomorphismRelation<String,LabeledEdge<String>>> {

	public class DiGMState {
		private String node1, node2;
		private int depth;

		public DiGMState(String n1, String n2) {
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

		private void predUpdate(NamedGraph<String,LabeledEdge<String>> g, Map<String,String> core, Map<String,Integer> in) {
			Set<String> newNodes = new HashSet<String>();
			for (String n : core.keySet()) {
				for (String label : m_labels) {
					for (String p : g.predecessors(n, label)) {
						if (!core.containsKey(p))
							newNodes.add(p);
					}
				}
			}
			for (String n : newNodes) {
				if (!in.containsKey(n)) {
					in.put(n, depth);
				}
			}
		}

		private void succUpdate(NamedGraph<String,LabeledEdge<String>> g, Map<String,String> core, Map<String,Integer> out) {
			Set<String> newNodes = new HashSet<String>();
			for (String n : core.keySet()) {
				for (String label : m_labels) {
					for (String p : g.successors(n, label)) {
						if (!core.containsKey(p))
							newNodes.add(p);
					}
				}
			}
			for (String n : newNodes) {
				if (!out.containsKey(n)) {
					out.put(n, depth);
				}
			}
		}

		private void remove(Map<String,Integer> set) {
			for (Iterator<String> i = set.keySet().iterator(); i.hasNext();) {
				String n = i.next();
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
		public String n1, n2;

		public Pair(String n1, String n2) {
			this.n1 = n1;
			this.n2 = n2;
		}

		public String toString() {
			return "(" + n1 + "," + n2 + ")";
		}

		@Override
		public int hashCode() {
//			final int prime = 31;
//			int result = 1;
//			result = prime * result + ((n1 == null) ? 0 : n1.hashCode());
//			result = prime * result + ((n2 == null) ? 0 : n2.hashCode());
			return (n1 != null ? n1.hashCode() : 0) + (n2 != null ? n2.hashCode() : 0);
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

	private NamedGraph<String,LabeledEdge<String>> g1, g2;
	private Set<String> m_labels;
	
	private Map<String,String> core1, core2;
	private Map<String,Integer> in1, in2, out1, out2;
	private DiGMState m_state;
	
	private List<IsomorphismRelation<String,LabeledEdge<String>>> m_isomorphisms;

	private FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> m_checker;
	private MappingListener<String,LabeledEdge<String>> m_listener;
	
	private int m_test;
	private boolean m_generateMappings;
	
	private final static Logger log = Logger.getLogger(DiGraphMatcher2.class);

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
	public DiGraphMatcher2(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings) {
		g1 = graph1;
		g2 = graph2;
		m_labels = new HashSet<String>();
		for (LabeledEdge<String> e : g1.edgeSet())
			m_labels.add(e.getLabel());

		core1 = new HashMap<String,String>();
		core2 = new HashMap<String,String>();
		in1 = new HashMap<String,Integer>();
		in2 = new HashMap<String,Integer>();
		out1 = new HashMap<String,Integer>();
		out2 = new HashMap<String,Integer>();

		m_state = new DiGMState(null, null);

		m_isomorphisms = new ArrayList<IsomorphismRelation<String,LabeledEdge<String>>>();
		m_test = TEST_GRAPH;
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
	public DiGraphMatcher2(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings,
			FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> checker) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
	}

	public DiGraphMatcher2(NamedGraph<String,LabeledEdge<String>> graph1,
			NamedGraph<String,LabeledEdge<String>> graph2, boolean generateMappings,
			FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> checker,
			MappingListener<String,LabeledEdge<String>> listener) {
		this(graph1, graph2, generateMappings);
		m_checker = checker;
		m_listener = listener;
	}

	public DiGraphMatcher2(NamedGraph<String,LabeledEdge<String>> graph1,
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
		m_test = TEST_SUBGRAPH;
		m_isomorphisms.clear();

		boolean found = match(m_state);

		return found;
	}

	/**
	 * Determines if <code>graph1</code> and <code>graph2</code> are isomorphic.
	 * 
	 * @return true, if <code>graph1</code> and <code>graph2</code> are
	 *         isomorphic, false otherwise
	 */
	public boolean isIsomorphic() {
		m_test = TEST_GRAPH;
		m_isomorphisms.clear();

		if (g1.vertexSet().size() != g2.vertexSet().size())
			return false;

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

		return new IsomorphismRelation<String,LabeledEdge<String>>(g1list, g2list, g1, g2);
	}

	static int mid = 0;
	private boolean match(DiGMState state) {
		if (core1.size() == g1.vertexSet().size()) {
			boolean valid = true;
			for (String n1 : core1.keySet())
				valid = m_checker.checkVertexCompatible(n1, core1.get(n1)) && valid; // lazy evaluation
			if (valid) {
				log.debug("found: " + core1);
				if (m_generateMappings) {
					IsomorphismRelation<String,LabeledEdge<String>> iso = createIsomorphismRelation(core1);
					m_isomorphisms.add(iso);
					if (m_listener != null)
						m_listener.mapping(iso);
//					try {
//						NamedGraph<String,LabeledEdge<String>> mg = new NamedGraph<String,LabeledEdge<String>>("mapping" + mid, new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
//						for (String x : core1.values()) {
//							for (String y : core1.values()) {
//								for (LabeledEdge<String> e : g2.getAllEdges(x, y)) {
//									mg.addEdge(x, e.getLabel(), y);
//								}
//							}
//						}
//						Util.printDOT(mg);
//						mid++;
//					} catch (StorageException e) {
//						e.printStackTrace();
//					}
				}
				return true;
			}
//			log.debug("invalid: " + core1);
			return false;
		} else {
			boolean found = false;
			Collection<Pair> candidatePairs = getCandidatePairs();
			// log.debug("cp: " + candidatePairs);
			for (Pair p : candidatePairs) {
				 log.debug("trying " + p + ", current mapping: " + core1);
				if (isFeasible(p.n1, p.n2)) {
					DiGMState newState = new DiGMState(p.n1, p.n2);
					if (match(newState)) {
						found = true;
						if (!m_generateMappings)
							return true;
					}
					
					newState.remove();
					
					if (isCurrentMappingInvalid())
						return false;
				}
			}

			return found;
		}
	}
	
	private boolean isCurrentMappingInvalid() {
		for (String n1 : core1.keySet())
			if (!m_checker.isVertexCompatible(n1, core1.get(n1)))
				return true;
		return false;
	}

	private SortedSet<String> getTerminalSet(NamedGraph<String,LabeledEdge<String>> g, Map<String,String> core, Map<String,Integer> inout) {
		SortedSet<String> set = new TreeSet<String>();
		for (String label : m_labels) {
			for (LabeledEdge<String> e : g.edgesByLabel(label)) {
				if (inout.containsKey(e.getSrc()) && !core.containsKey(e.getSrc()))
					set.add(e.getSrc());
				if (inout.containsKey(e.getDst()) && !core.containsKey(e.getDst()))
					set.add(e.getDst());
			}
		}
//		for (String n : g.vertexSet()) {
//			if (inout.containsKey(n) && !core.containsKey(n))
//				set.add(n);
//		}
		return set;
	}

	private List<Pair> getCandidatePairs() {
		List<Pair> candidates = new LinkedList<Pair>();

		SortedSet<String> t1out = getTerminalSet(g1, core1, out1);
		SortedSet<String> t2out = getTerminalSet(g2, core2, out2);

		if (t1out.size() != 0 && t2out.size() != 0) {
			String n1 = t1out.first();
			for (String n2 : t2out)
				if (m_checker.isVertexCompatible(n1, n2))
					candidates.add(new Pair(n1, n2));
		} 
		else {
			SortedSet<String> t1in = getTerminalSet(g1, core1, in1);
			SortedSet<String> t2in = getTerminalSet(g2, core2, in2);

			if (t1in.size() != 0 && t2in.size() != 0) {
				String n1 = t1in.first();
				for (String n2 : t2in)
					if (m_checker.isVertexCompatible(n1, n2))
						candidates.add(new Pair(n1, n2));
			} 
			else if (t1in.size() == 0 && t2in.size() == 0) {
//				TreeSet<String> diff = new TreeSet<String>(g1.vertexSet());
//				diff.removeAll(core1.keySet());
//				String n1 = diff.first();
				String n1 = null;
				for (String v : g1.sortedVertexSet())
					if (!core1.containsKey(v)) {
						n1 = v;
						break;
					}
				
				for (String n2 : g2.vertexSet())
					if (!core2.containsKey(n2))
						if (m_checker.isVertexCompatible(n1, n2))
							candidates.add(new Pair(n1, n2));
			}
		}

		return candidates;
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
		List<LabeledEdge<String>> l1, l2;

		public Tuple(List<LabeledEdge<String>> l1, List<LabeledEdge<String>> l2) {
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

	private boolean edgeSetsCompatible(List<LabeledEdge<String>> g1edges, List<LabeledEdge<String>> g2edges) {
		Tuple t = new Tuple(g1edges, g2edges);
		Boolean val = edgeSetCache.get(t);
		if (val == null) {
			for (LabeledEdge<String> e1 : g1edges) {
				boolean found = false;
				for (LabeledEdge<String> e2 : g2edges) {
					if (m_checker.isEdgeCompatible(e1, e2)) {
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
	
	private boolean isFeasibleIso(String n1, String n2) {
		// R_self
		// if (g1.getAllEdges(n1, n1).size() != g2.getAllEdges(n2, n2).size())
		// return false;

		// R_pred
		Map<String,List<LabeledEdge<String>>> n1preds = g1.predecessorEdges(n1);
		// for (String n1pred : predecessors(g1, n1)) {
		for (String n1pred : n1preds.keySet()) {
			if (core1.containsKey(n1pred)) {
//				 Set<LabeledEdge<String>> g1edges = g1.getAllEdges(n1pred, n1);
				List<LabeledEdge<String>> g1edges = n1preds.get(n1pred);
				// Set<String> n2preds = predecessors(g2, n2);
				Map<String,List<LabeledEdge<String>>> n2preds = g2.predecessorEdges(n2);

				if (g1edges.size() > 0) {
					// for (String n2pred : n2preds) {
					for (String n2pred : n2preds.keySet()) {
						if (n2pred.equals(core1.get(n1pred))) {
							// Set<LabeledEdge<String>> g2edges =
							// g2.getAllEdges(core1.get(n1pred), n2);
							List<LabeledEdge<String>> g2edges = n2preds.get(n2pred);
							if (g1edges.size() > g2edges.size())
								return false;
							if (!edgeSetsCompatible(g1edges, g2edges))
								return false;
						}
					}
				} else {
					log.error("should not happen, g1edges should be > 0");
				}
				// if (!n2preds.contains(core1.get(n1pred)))
				// return false;
				// else if (g1.getAllEdges(n1pred, n1).size() >
				// g2.getAllEdges(core1.get(n1pred), n2).size())
				// return false;
			}
		}

		// for (String n2pred : predecessors(g2, n2)) {
		// if (core2.containsKey(n2pred)) {
		// Set<String> n1preds = predecessors(g1, n1);
		// if (!n1preds.contains(core2.get(n2pred)))
		// return false;
		// else if (g2.getAllEdges(n2pred, n2).size() !=
		// g1.getAllEdges(core2.get(n2pred), n1).size())
		// return false;
		// }
		// }

		// R_succ
		for (String n1succ : g1.successors(n1)) {
			if (core1.containsKey(n1succ)) {
				Set<LabeledEdge<String>> g1edges = g1.getAllEdges(n1, n1succ);
				Set<String> n2succs = g2.successors(n2);

				if (g1edges.size() > 0) {
					for (String n2succ : n2succs) {
						if (n2succ.equals(core1.get(n1succ))) {
							Set<LabeledEdge<String>> g2edges = g2.getAllEdges(n2,
									core1.get(n1succ));
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

				} else {
					log.error("should not happen, g1edges should be > 0");
				}
				// if (!n2succs.contains(core1.get(n1succ)))
				// return false;
				// else if (g1.getAllEdges(n1succ, n1).size() >
				// g2.getAllEdges(core1.get(n1succ), n2).size())
				// return false;
			}
		}

		// for (String n2succ : successors(g2, n2)) {
		// if (core2.containsKey(n2succ)) {
		// Set<String> n1succs = successors(g1, n1);
		// if (!n1succs.contains(core2.get(n2succ)))
		// return false;
		// else if (g2.getAllEdges(n2succ, n2).size() !=
		// g1.getAllEdges(core2.get(n2succ), n1).size())
		// return false;
		// }
		// }

		// R_termin
		int num1 = count(g1.predecessors(n1), in1.keySet(), core1.keySet());
		int num2 = count(g2.predecessors(n2), in2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;

		num1 = count(g1.successors(n1), in1.keySet(), core1.keySet());
		num2 = count(g2.successors(n2), in2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;

		// R_termout
		num1 = count(g1.predecessors(n1), out1.keySet(), core1.keySet());
		num2 = count(g2.predecessors(n2), out2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;

		num1 = count(g1.successors(n1), out1.keySet(), core1.keySet());
		num2 = count(g2.successors(n2), out2.keySet(), core2.keySet());
		if (!compareCount(num1, num2))
			return false;

		// R_new
		num1 = count(g1.predecessors(n1), in1.keySet(), out1.keySet());
		num2 = count(g2.predecessors(n2), in2.keySet(), out2.keySet());
		if (!compareCount(num1, num2))
			return false;

		num1 = count(g1.successors(n1), in1.keySet(), out1.keySet());
		num2 = count(g2.successors(n2), in2.keySet(), out2.keySet());
		if (!compareCount(num1, num2))
			return false;

		return true;
	}

	private boolean isFeasibleSubgraph(String n1, String n2) {
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
		Map<String,List<LabeledEdge<String>>> n1predMap = g1.predecessorEdges(n1);
		Map<String,List<LabeledEdge<String>>> n2predMap = g2.predecessorEdges(n2);
		for (String n1pred : n1predMap.keySet()) {
			String n1mapped = core1.get(n1pred);
			if (n1mapped != null) {
				List<LabeledEdge<String>> n1predEdges = n1predMap.get(n1pred);

				if (!n2predMap.keySet().contains(n1mapped))
					return false;
				else if (n1predEdges.size() > n2predMap.get(n1mapped).size())
					return false;

				List<LabeledEdge<String>> g1edges = n1predEdges;

				boolean found = false;
				for (String n2pred : n2predMap.keySet()) {
					if (core2.containsKey(n2pred)) {
						List<LabeledEdge<String>> g2edges = n2predMap.get(n2pred);
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

		for (String n2pred : n2predMap.keySet()) {
			if (core2.containsKey(n2pred)) {
				// monomorphism: do nothing
			} else {
				if (in2.containsKey(n2pred))
					termin2++;
				if (out2.containsKey(n2pred))
					termout2++;
				if (!in2.containsKey(n2pred) && !out2.containsKey(n2pred))
					new2++;
			}
		}

		// R_succ
		Map<String,List<LabeledEdge<String>>> n1succs = g1.successorEdges(n1);
		Map<String,List<LabeledEdge<String>>> n2succs = g2.successorEdges(n2);
		for (String n1succ : n1succs.keySet()) {
			String n1mapped = core1.get(n1succ);
			if (n1mapped != null) {
				List<LabeledEdge<String>> n1succEdges = n1succs.get(n1succ);

				if (!n2succs.keySet().contains(n1mapped))
					return false;
				else if (n1succEdges.size() > n2succs.get(n1mapped).size())
					return false;

				List<LabeledEdge<String>> g1edges = n1succEdges;

				boolean found = false;
				for (String n2succ : n2succs.keySet()) {
					if (core2.containsKey(n2succ)) {
						List<LabeledEdge<String>> g2edges = n2succs.get(n2succ);
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

		// for (String n2succ : successors(g2, n2)) {
		for (String n2succ : n2succs.keySet()) {
			if (core2.containsKey(n2succ)) {
				// monomorphism: do nothing
			} else {
				if (in2.containsKey(n2succ))
					termin2++;
				if (out2.containsKey(n2succ))
					termout2++;
				if (!in2.containsKey(n2succ) && !out2.containsKey(n2succ))
					new2++;
			}
		}

		log.debug(in1 + " " + in2 + " " + out1 + " " + out2);
		log.debug(termin1 + " " + termin2 + " " + termout1 + " " + termout2 + " " + new1 + " " + new2 + " " + core1);
		return termin1 <= termin2 && termout1 <= termout2
				&& (termin1 + termout1 + new1) <= (termin2 + termout2 + new2);
	}

	private boolean isFeasible(String n1, String n2) {
		
		if (core1.size() == 0) {
			if (!m_checker.isVertexCompatible(n1, n2))
				return false;
		}
		else {
			if (!m_checker.checkVertexCompatible(n1, n2))
				return false;
		}

		if (m_test == TEST_GRAPH)
			return isFeasibleIso(n1, n2);
		else
			return isFeasibleSubgraph(n1, n2);
	}

	private boolean compareCount(int num1, int num2) {
		if (m_test == TEST_GRAPH) {
			if (num1 != num2)
				return false;
		} else if (num1 < num2)
			return false;
		return true;
	}

	private int count(Set<String> nodes, Set<String> inout, Set<String> core) {
		int n = 0;
		for (String node : nodes) {
			if (inout.contains(node) && !core.contains(node))
				n++;
		}
		return n;
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
