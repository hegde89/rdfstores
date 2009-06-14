package edu.unika.aifb.graphindex.query.model;

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
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

public class Query {
	private List<Literal> m_literals;
	private List<String> m_selectVariables;
	private String m_name;
	private Set<String> m_backwardEdgeSet, m_forwardEdgeSet, m_neutralEdgeSet;
	private Map<String,Integer> m_e2s;
	private Set<String> m_removeNodes;
	private Graph<QueryNode> m_queryGraph;
	private Set<String> m_backwardTargets;
	private Set<String> m_forwardSources;
	private int m_longestPathFromConstant = 0;
	private boolean m_ignoreIndexEdgeSets = false;
	private ArrayList<List<GraphEdge<QueryNode>>> m_prunedParts;
	private static final Logger log = Logger.getLogger(Query.class);
	
	public Query(List<String> vars) {
		m_literals = new ArrayList<Literal>();
		m_selectVariables = vars;
		m_removeNodes = new HashSet<String>();
		m_backwardEdgeSet = new HashSet<String>();
		m_forwardEdgeSet = new HashSet<String>();
		m_forwardSources = new HashSet<String>();
		m_neutralEdgeSet = new HashSet<String>();
		m_backwardTargets = new HashSet<String>();
	}
	
	public Set<String> getBackwardEdgeSet() {
		return m_backwardEdgeSet;
	}
	
	public Set<String> getForwardEdgeSet() {
		return m_forwardEdgeSet;
	}
	
	public Set<String> getNeutralEdgeSet() {
		return m_neutralEdgeSet;
	}
	
	public String getName() {
		return m_name;
	}
	
	public void setName(String name) {
		m_name = name;
	}
	
	public Set<String> getRemovedNodes() {
		return m_removeNodes;
	}
	
	public void setRemoveNodes(Set<String> rn) {
		m_removeNodes = rn;
	}
	
	public void setSelectVariables(List<String> vars) {
		m_selectVariables = vars;
	}
	
	public List<String> getSelectVariables() {
		return m_selectVariables;
	}
	
	public List<Literal> getLiterals() {
		return m_literals;
	}

	public boolean addLiteral(Literal o) {
		return m_literals.add(o);
	}
	
	public Map<String,Integer> getEvalOrder() {
		return m_e2s;
	}
	
	public void setEvalOrder(Map<String,Integer> e2s) {
		m_e2s = e2s;
	}
	
	public int getLongestPathFromConstant() {
		return m_longestPathFromConstant;
	}
	
	public String toSPARQL() {
		String s = "SELECT ";
		for (String sv : m_selectVariables)
			s += sv + " ";
		s += " WHERE {\n";
		for (Literal l : m_literals) {
			String sub = l.getSubject().toString();
			String p = "<" + l.getPredicate().toString() + ">";
			String obj = l.getObject().toString();
			
			if (sub.startsWith("http"))
				sub = "<" + sub + ">";
			
			if (obj.startsWith("http"))
				obj = "<" + obj + ">";
			else if (!obj.startsWith("?"))
				obj = "'" + obj + "'";
			
			s += sub + " " + p + " " + obj + " . \n";
		}
		s += "}";
		return s;
	}

	public NamedQueryGraph<String,LabeledQueryEdge<String>> toQueryGraph() throws StorageException {
		NamedQueryGraph<String,LabeledQueryEdge<String>> g = new NamedQueryGraph<String,LabeledQueryEdge<String>>("query", new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledQueryEdge.class));
		for (Literal l : m_literals) {
			g.addEdge(l.getSubject(), l.getPredicate(), l.getObject());
		}
		return g;
	}
	
	public void createQueryGraph(StructureIndex index) {
		DirectedGraph<QueryNode,LabeledEdge<QueryNode>> g = new DirectedMultigraph<QueryNode,LabeledEdge<QueryNode>>(new ClassBasedEdgeFactory<QueryNode,LabeledEdge<QueryNode>>((Class<? extends LabeledEdge<QueryNode>>)LabeledEdge.class));
		Map<String,QueryNode> t2qn = new HashMap<String,QueryNode>();
		Set<String> edgeLabels = new HashSet<String>();
		Set<QueryNode> constants = new HashSet<QueryNode>();
		for (Literal l : m_literals) {
			QueryNode src = t2qn.get(l.getSubject().toString());
			if (src == null) {
				src = new QueryNode(l.getSubject().toString());
				src.addMember(l.getSubject().toString());
				src.setTerm(l.getSubject().toString(), l.getSubject());
				t2qn.put(l.getSubject().toString(), src);
				g.addVertex(src);
			}

			QueryNode dst = t2qn.get(l.getObject().toString());
			if (dst == null) {
				dst = new QueryNode(l.getObject().toString());
				dst.addMember(l.getObject().toString());
				dst.setTerm(l.getObject().toString(), l.getObject());
				t2qn.put(l.getObject().toString(), dst);
				g.addVertex(dst);
			}
			
			if (Util.isConstant(dst.getName()))
				constants.add(dst);
			
			edgeLabels.add(l.getPredicate().getUri());
			g.addEdge(src, dst, new LabeledEdge<QueryNode>(src, dst, l.getPredicate().getUri()));
		}
		
		m_queryGraph = new Graph<QueryNode>(g);
		
		if (index != null) {
			Set<String> indexEdges = new HashSet<String>();
			indexEdges.addAll(index.getBackwardEdges());
			indexEdges.addAll(index.getForwardEdges());
			if (!m_ignoreIndexEdgeSets && !indexEdges.containsAll(edgeLabels))
				m_queryGraph = null;
			else {
				pruneQueryGraph(m_queryGraph, index);
				Map<String,List<GraphEdge<QueryNode>>> parts = new HashMap<String,List<GraphEdge<QueryNode>>>();
				for (GraphEdge<QueryNode> edge : m_prunedEdges) {
					String s = m_queryGraph.getSourceNode(edge).getName();
					String d = m_queryGraph.getTargetNode(edge).getName();
					
					if (parts.containsKey(s) && parts.containsKey(d)) {
						List<GraphEdge<QueryNode>> slist = parts.get(s);
						List<GraphEdge<QueryNode>> dlist = parts.get(d);
						slist.add(edge);
						if (slist != dlist) {
							slist.addAll(dlist);
							for (GraphEdge<QueryNode> e : dlist) {
								parts.put(m_queryGraph.getSourceNode(e).getName(), slist);
								parts.put(m_queryGraph.getTargetNode(e).getName(), slist);
							}
						}
					}
					else if (parts.containsKey(s) && !parts.containsKey(d) && m_removeNodes.contains(s)) {
						List<GraphEdge<QueryNode>> list = parts.get(s);
						list.add(edge);
						if (m_removeNodes.contains(d))
							parts.put(d, list);
					}
					else if (!parts.containsKey(s) && parts.containsKey(d) && m_removeNodes.contains(d)) {
						List<GraphEdge<QueryNode>> list = parts.get(d);
						list.add(edge);
						if (m_removeNodes.contains(s))
							parts.put(s, list);
					}
					else {
						List<GraphEdge<QueryNode>> list = new ArrayList<GraphEdge<QueryNode>>();
						list.add(edge);
						if (m_removeNodes.contains(s))
							parts.put(s, list);
						if (m_removeNodes.contains(d))
							parts.put(d, list);
					}
						
				}
				m_prunedParts = new ArrayList<List<GraphEdge<QueryNode>>>();
				int maxLength = 0;
				for (List<GraphEdge<QueryNode>> list : parts.values()) {
					if (list.size() > maxLength)
						maxLength = list.size();
					if (!m_prunedParts.contains(list))
						m_prunedParts.add(list);
				}
//				log.debug(getName() + ": " + m_removeNodes + " " +  maxLength);
			}
		}
		
//		int longestPath = 0;
//		for (QueryNode constant : constants) {
//			Stack<List<Integer>> toVisit = new Stack<List<Integer>>();
//			toVisit.push(Arrays.asList(m_queryGraph.getNodeId(constant)));
//			while (toVisit.size() > 0) {
//				List<Integer> path = toVisit.pop();
//				
//				if (path.size() - 1 > longestPath)
//					longestPath = path.size() - 1;
//				
//				for (int succ : m_queryGraph.successors(path.get(path.size() - 1)))
//					if (!path.contains(succ)) {
//						List<Integer> next = new ArrayList<Integer>(path);
//						next.add(succ);
//						toVisit.push(next);
//					}
//				
//				for (int pred : m_queryGraph.predecessors(path.get(path.size() - 1)))
//					if (!path.contains(pred)) {
//						List<Integer> next = new ArrayList<Integer>(path);
//						next.add(pred);
//						toVisit.push(next);
//					}
//			}
//		}
//		m_longestPathFromConstant = longestPath;
	}
	
	private int calcDistances(List<GraphEdge<QueryNode>> part, String node, int distance, Map<String,Integer> distances) {
		distances.put(node, distance);
		
		int max = distance;
		for (GraphEdge<QueryNode> edge : part) {
			
			String src = m_queryGraph.getSourceNode(edge).getName();
			String trg = m_queryGraph.getTargetNode(edge).getName();
			
			if (src.equals(node) && !distances.containsKey(trg)) {
				int dist = calcDistances(part, trg, distance + 1, distances);
				if (dist > max)
					max = dist;
			}
			if (trg.equals(node) && !distances.containsKey(src)) {
				int dist = calcDistances(part, src, distance + 1, distances);
				if (dist > max)
					max = dist;
			}
		}
		
		return max;
	}
	
	public void trimPruning(int pathLength) {
		log.debug("trimming parts to length " + pathLength);
		log.debug("parts before: " + m_prunedParts);
		
		m_forwardSources.clear();
		m_backwardTargets.clear();
		
		Set<GraphEdge<QueryNode>> reclaimedEdges = new HashSet<GraphEdge<QueryNode>>();
		Set<String> reclaimedNodes = new HashSet<String>();
		
		for (List<GraphEdge<QueryNode>> part : m_prunedParts) {
			if (part.size() > pathLength) {
				log.debug(part);
				String startNode = null;
				for (GraphEdge<QueryNode> edge : part) {
					if (!m_removeNodes.contains(m_queryGraph.getSourceNode(edge).getName()))
						startNode = m_queryGraph.getSourceNode(edge).getName();
					if (!m_removeNodes.contains(m_queryGraph.getTargetNode(edge).getName()))
						startNode = m_queryGraph.getTargetNode(edge).getName();
				}
				log.debug("start node: " + startNode);
				
				Map<String,Integer> distances = new HashMap<String,Integer>();
				int max = calcDistances(part, startNode, 0, distances);
				int reclaimDistance = max - pathLength;
				log.debug(distances + " " + max + " " + reclaimDistance);
				
				if (reclaimDistance > 0) {
					for (String node : distances.keySet()) {
						if (distances.get(node) <= reclaimDistance && m_removeNodes.contains(node))
							reclaimedNodes.add(node);
					}
					
					log.debug(" reclaim node: " + reclaimedNodes);

					Set<String> notRemovedNodes = new HashSet<String>(reclaimedNodes);
					notRemovedNodes.add(startNode);
					
					for (Iterator<GraphEdge<QueryNode>> i = part.iterator(); i .hasNext(); ) {
						GraphEdge<QueryNode> edge = i.next();
						String src = m_queryGraph.getSourceNode(edge).getName();
						String trg = m_queryGraph.getTargetNode(edge).getName();
						
						if (notRemovedNodes.contains(src) && notRemovedNodes.contains(trg)) {
							i.remove();
							reclaimedEdges.add(edge);
						}
					}
					
					log.debug(" reclaimed edges: " + reclaimedEdges);
				}
			}
		}
		
		
		m_removeNodes.removeAll(reclaimedNodes);
		m_prunedEdges.removeAll(reclaimedEdges);
		
		for (GraphEdge<QueryNode> edge : m_queryGraph.edges()) {
			String src = m_queryGraph.getSourceNode(edge).getName();
			String trg = m_queryGraph.getTargetNode(edge).getName();

			if (!m_removeNodes.contains(src) && m_removeNodes.contains(trg))
				m_forwardSources.add(src);
			if (m_removeNodes.contains(src) && !m_removeNodes.contains(trg))
				m_backwardTargets.add(trg);
		}

		log.debug("rem: " + m_removeNodes);
		log.debug("srcs: " + m_forwardSources);
		log.debug("trgs: "+ m_backwardTargets);
		log.debug("e: " + m_prunedEdges);
		log.debug("parts after: " + m_prunedParts);
	}
	
	public Graph<QueryNode> getGraph() {
		return m_queryGraph;
	}
	
	public String toString() {
		String s = "(select: " + m_selectVariables + ", remove: " + m_removeNodes + ")\n";
		String nl = "";
		for (Literal l : m_literals) {
			s += nl + l.getSubject() + " " + l.getPredicate() + " " + l.getObject();
			nl = "\n";
		}
		return s;
	}
	
	public Map<String,Term> getTerms() {
		Map<String,Term> terms = new HashMap<String,Term>();
		for (Literal l : m_literals) {
			terms.put(l.getSubject().toString(), l.getSubject());
			terms.put(l.getObject().toString(), l.getObject());
		}
		return terms;
	}

	public Set<String> getVariables() {
		Set<String> vars = new HashSet<String>();
		for (Literal l : m_literals) {
			if (l.getSubject() instanceof Variable)
				vars.add(l.getSubject().toString());
			if (l.getObject() instanceof Variable)
				vars.add(l.getObject().toString());
		}
		return vars;
	}
	
	@SuppressWarnings("unchecked")
	private void pruneQueryGraph(Graph<QueryNode> g, StructureIndex index) {
		m_removeNodes = new HashSet<String>();
		m_neutralEdgeSet = new HashSet<String>();
		
		resetEdgeSets();
		
		Set<Integer> fixedNodes = new HashSet<Integer>();
		Set<Integer> selectIds = new HashSet<Integer>();
		
		for (String srcSelectVar : m_selectVariables) {
			int srcId = g.getNodeId(new QueryNode(srcSelectVar));
			selectIds.add(srcId);
			calculateFixedPaths(g, fixedNodes, srcId);
		}
		
		m_neutralEdgeSet.remove("");
		
		Set<String> fixed = new HashSet<String>();
		for (int n : fixedNodes)
			fixed.add(g.getNode(n).getName());
//		log.debug("fixed: " + fixed);

		m_removeNodes.removeAll(fixed);
		
		Map<String,Set<String>> bwEdgeSources = new HashMap<String,Set<String>>();
		Map<String,Set<String>> fwEdgeTargets = new HashMap<String,Set<String>>();
		
		calculateEdgeSets(g, index, fixedNodes, new HashSet<Integer>(fixedNodes), bwEdgeSources, fwEdgeTargets);

		for (int n : fixedNodes)
			fixed.add(g.getNode(n).getName());

		m_removeNodes.removeAll(fixed);

//		log.debug("bw: " + m_backwardEdgeSet);
//		log.debug("fw: " + m_forwardEdgeSet);
//		log.debug("bwedgesources: " + bwEdgeSources);
//		log.debug("fwedgetargets: " + fwEdgeTargets);
		
//		if (index != null) {
//			Set<String> unIndexedForwardEdges = new HashSet<String>(m_forwardEdgeSet);
//			unIndexedForwardEdges.removeAll(index.getForwardEdges());
//			log.debug("unindexed fw: " + unIndexedForwardEdges);
//			
//			Set<String> notPrunable = new HashSet<String>();
//			for (String edge : unIndexedForwardEdges) {
//				if (fwEdgeTargets.containsKey(edge)) {
//					for (String node : fwEdgeTargets.get(edge)) {
//						Set<String> dependsOn = depends.get(node);
//						log.debug(" not prunable fw node: " + node + ", depends on: " + dependsOn);
//						notPrunable.add(node);
//					}
//				}
//			}
//	
//			Set<String> unIndexedBackwardEdges = new HashSet<String>(m_backwardEdgeSet);
//			unIndexedBackwardEdges.removeAll(index.getBackwardEdges());
//			log.debug("unindexed bw: " + unIndexedBackwardEdges);
//			
//			for (String edge : unIndexedBackwardEdges) {
//				if (bwEdgeSources.containsKey(edge)) {
//					for (String node : bwEdgeSources.get(edge)) {
//						notPrunable.add(node);
//					}
//				}
//			}
//			
//			for (String node : notPrunable) {
//				int id = g.getNodeId(new QueryNode(node));
//				fixedNodes.add(id);
//				selectIds.add(id);
//			}
//
//			Set<String> newFixed = new HashSet<String>();
//			for (int n : fixedNodes)
//				newFixed.add(g.getNode(n).getName());
////			log.debug("fixed: " + newFixed);
//			m_removeNodes.removeAll(newFixed);
////			log.debug("remove: " + m_removeNodes);
//			
//			bwEdgeSources = new HashMap<String,Set<String>>();
//			fwEdgeTargets = new HashMap<String,Set<String>>();
//			resetEdgeSets();
//			calculateEdgeSets(g, index, fixedNodes, selectIds, bwEdgeSources, fwEdgeTargets);
//		}		
		

//		log.debug(getName() + " queryrem: " + m_removeNodes.size() + " " + m_removeNodes + ", fixed: " + fixed);
	}

	private void calculateFixedPaths(Graph<QueryNode> g, Set<Integer> fixedNodes, int srcId) {
		Set<Integer> visited = new HashSet<Integer>();
		Stack<Integer> currentPath = new Stack<Integer>();
		Stack<String> currentLabelPath = new Stack<String>();
		Stack<Integer[]> toVisit = new Stack<Integer[]>();

		toVisit.push(new Integer[] {srcId, 0});
		currentLabelPath.push("");
		while (toVisit.size() > 0) {
			Integer[] cur = toVisit.pop();
			int n = cur[0];
			int level = cur[1];
			QueryNode qn = g.getNode(n);
			
			m_removeNodes.add(qn.getName());
			
			if (visited.contains(n))
				continue;
			visited.add(n);
			for (int i = currentPath.size() - 1; i >= level; i--) {
				currentPath.remove(i);
				currentLabelPath.remove(i);
			}
			currentPath.push(n);
//				log.debug(currentPath);
//				log.debug(currentLabelPath);
			if (n != srcId) {
				if (m_selectVariables.contains(qn.getName()) || !qn.getName().startsWith("?")) {
					fixedNodes.addAll(currentPath);
					m_neutralEdgeSet.addAll(currentLabelPath);
				}
			}
			
			int toVisitSize = toVisit.size();
			
			for (GraphEdge<QueryNode> edge : g.incomingEdges(n)) {
				if (!visited.contains(edge.getSrc())) {
					toVisit.push(new Integer[] {edge.getSrc(), level + 1});
					currentLabelPath.add(edge.getLabel());
				}
			}
			for (GraphEdge<QueryNode> edge : g.outgoingEdges(n)) {
				if (!visited.contains(edge.getDst())) {
					toVisit.push(new Integer[] {edge.getDst(), level + 1});
					currentLabelPath.add(edge.getLabel());
				}
			}
//				log.debug(toVisit);
//				if (toVisitSize == toVisit.size()) {
//					currentPath.pop();
//				}
		}
	}

	private void resetEdgeSets() {
		m_forwardEdgeSet = new HashSet<String>();
		m_backwardEdgeSet  = new HashSet<String>();
		m_forwardSources = new HashSet<String>();
		m_backwardTargets = new HashSet<String>();
		m_prunedEdges = new HashSet<GraphEdge<QueryNode>>();
	}
	
	private Set<GraphEdge<QueryNode>> m_prunedEdges = new HashSet<GraphEdge<QueryNode>>();

	private void calculateEdgeSets(Graph<QueryNode> g, StructureIndex index, Set<Integer> fixedNodes, Set<Integer> selectIds, Map<String,Set<String>> bwEdgeSources, Map<String,Set<String>> fwEdgeTargets) {
		for (int srcId : selectIds) {
			Set<Integer> visited = new HashSet<Integer>();
			Stack<Integer> toVisit = new Stack<Integer>();
			
			toVisit.push(srcId);
			visited.add(srcId);
			int startDirection = 0; // 0 = none, 1 = bw, 2 = fw
			while (toVisit.size() > 0) {
				int n = toVisit.pop();
				visited.add(n);
				QueryNode qn = g.getNode(n);
				
				for (GraphEdge<QueryNode> edge : g.incomingEdges(n)) {
					if (!visited.contains(edge.getSrc()) && !fixedNodes.contains(edge.getSrc())) {
						toVisit.add(edge.getSrc());
						
						if (!m_ignoreIndexEdgeSets && !index.getBackwardEdges().contains(edge.getLabel())) {
							fixedNodes.add(edge.getSrc());
							calculateFixedPaths(g, fixedNodes, edge.getSrc());
							continue;
						}
						
						m_prunedEdges.add(edge);

						m_backwardEdgeSet.add(edge.getLabel());
						if (startDirection == 2)
							m_forwardEdgeSet.add(edge.getLabel());
						
						if (!bwEdgeSources.containsKey(edge.getLabel()))
							bwEdgeSources.put(edge.getLabel(), new HashSet<String>());
						bwEdgeSources.get(edge.getLabel()).add(g.getNode(edge.getSrc()).getSingleMember());
						
						if (fixedNodes.contains(n))
							m_backwardTargets.add(qn.getName());
						
						if (startDirection == 0)
							startDirection = 1;
					}
				}
				for (GraphEdge<QueryNode> edge : g.outgoingEdges(n)) {
					if (!visited.contains(edge.getDst()) && !fixedNodes.contains(edge.getDst())) {
						toVisit.add(edge.getDst());

						if (!m_ignoreIndexEdgeSets && !index.getForwardEdges().contains(edge.getLabel())) {
							fixedNodes.add(edge.getDst());
							calculateFixedPaths(g, fixedNodes, edge.getDst());
							continue;
						}

						m_prunedEdges.add(edge);
						
						m_forwardEdgeSet.add(edge.getLabel());
						if (startDirection == 1)
							m_backwardEdgeSet.add(edge.getLabel());
						
						if (!fwEdgeTargets.containsKey(edge.getLabel()))
							fwEdgeTargets.put(edge.getLabel(), new HashSet<String>());
						fwEdgeTargets.get(edge.getLabel()).add(g.getNode(edge.getDst()).getSingleMember());
						
						if (fixedNodes.contains(n))
							m_forwardSources.add(qn.getName());

						if (startDirection == 0)
							startDirection = 2;
					}
				}
			}
		}
	}

	public Map<String,Integer> calculateConstantProximities() {
		Set<Integer> visited = new HashSet<Integer>();
		int startNode = -1;
		final Map<String,Integer> scores = new HashMap<String,Integer>();
		for (int i = 0; i < m_queryGraph.nodeCount(); i++) {
			String node = m_queryGraph.getNode(i).getSingleMember();
			if (!node.startsWith("?")) {
				scores.put(node, 0);
				startNode = i;
			}
		}
		
		if (startNode == -1)
			startNode = 0;
		
		Stack<Integer> tov = new Stack<Integer>();
		
		tov.push(startNode);
		
		while (tov.size() > 0) {
			int node = tov.pop();
			
			if (visited.contains(node))
				continue;
			visited.add(node);

			String curNode = m_queryGraph.getNode(node).getSingleMember();
			
			int min = Integer.MAX_VALUE;
			for (int i : m_queryGraph.predecessors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = m_queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			for (int i : m_queryGraph.successors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = m_queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			if (!scores.containsKey(curNode))
				scores.put(curNode, min + 1);
		}
		
		return scores;
	}
	
	public Set<String> getBackwardTargets() {
		return m_backwardTargets;
	}
	
	public Set<String> getForwardSources() {
		return m_forwardSources;
	}
	
	public void setForwardSources(Set<String> sources) {
		m_forwardSources = sources;
	}

	public void setIgnoreIndexEdgeSets(boolean m_ignoreIndexEdgeSets) {
		this.m_ignoreIndexEdgeSets = m_ignoreIndexEdgeSets;
	}

	public boolean ignoreIndexEdgeSets() {
		return m_ignoreIndexEdgeSets;
	}
}
