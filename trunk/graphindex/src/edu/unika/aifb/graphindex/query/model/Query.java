package edu.unika.aifb.graphindex.query.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.storage.StorageException;

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
	
	public void createQueryGraph() {
		DirectedGraph<QueryNode,LabeledEdge<QueryNode>> g = new DirectedMultigraph<QueryNode,LabeledEdge<QueryNode>>(new ClassBasedEdgeFactory<QueryNode,LabeledEdge<QueryNode>>((Class<? extends LabeledEdge<QueryNode>>)LabeledEdge.class));
		Map<String,QueryNode> t2qn = new HashMap<String,QueryNode>();
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

			g.addEdge(src, dst, new LabeledEdge<QueryNode>(src, dst, l.getPredicate().getUri()));
		}
		
		m_queryGraph = new Graph<QueryNode>(g);
		pruneQueryGraph(m_queryGraph);
	}
	
	public Graph<QueryNode> getGraph() throws StorageException {
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
	private void pruneQueryGraph(Graph<QueryNode> g) {
		Set<Integer> fixedNodes = new HashSet<Integer>();
		Set<Integer> selectIds = new HashSet<Integer>();
		m_removeNodes = new HashSet<String>();
		
		for (String srcSelectVar : m_selectVariables) {
			Set<Integer> visited = new HashSet<Integer>();
			Stack<Integer> currentPath = new Stack<Integer>();
			Stack<String> currentLabelPath = new Stack<String>();
			Stack<Integer[]> toVisit = new Stack<Integer[]>();

			int srcId = g.getNodeId(new QueryNode(srcSelectVar));
			fixedNodes.add(srcId);
			selectIds.add(srcId);
			
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
		
		m_neutralEdgeSet.remove("");
		
		Set<String> fixed = new HashSet<String>();
		for (int n : fixedNodes)
			fixed.add(g.getNode(n).getName());
//		log.debug("fixed: " + fixed);
		
		m_removeNodes.removeAll(fixed);
		
		Set<String> bw = new HashSet<String>();
		Set<String> fw = new HashSet<String>();
		Set<String>[] removableNodeSets = new Set[] { new HashSet<String>(), new HashSet<String>() };
		for (int srcId : selectIds) {
//			List<Integer>[] startNodeSets = new List[] {g.predecessors(srcId), g.successors(srcId)};
			List<Integer>[] startNodeSets = new List[] {Arrays.asList(srcId), Arrays.asList(srcId)};
			for (int i = 0; i < startNodeSets.length; i++) {
				Set<Integer> visited = new HashSet<Integer>();
				Stack<Integer> toVisit = new Stack<Integer>();
				
				toVisit.addAll(startNodeSets[i]);
				visited.add(srcId);
				while (toVisit.size() > 0) {
					int n = toVisit.pop();
					visited.add(n);
					QueryNode qn = g.getNode(n);
					
					if (!fixedNodes.contains(n))
						removableNodeSets[i].add(qn.getName());
					
					for (GraphEdge<QueryNode> edge : g.incomingEdges(n)) {
						if (!visited.contains(edge.getSrc()) && !fixedNodes.contains(edge.getSrc())) {
							toVisit.add(edge.getSrc());
							m_backwardEdgeSet.add(edge.getLabel());
							if (fixedNodes.contains(n))
								m_backwardTargets.add(qn.getName());
						}
					}
					for (GraphEdge<QueryNode> edge : g.outgoingEdges(n)) {
						if (!visited.contains(edge.getDst()) && !fixedNodes.contains(edge.getDst())) {
							toVisit.add(edge.getDst());
							m_forwardEdgeSet.add(edge.getLabel());
							if (fixedNodes.contains(n))
								m_forwardSources.add(qn.getName());
						}
					}
				}
			}
		}
		bw = removableNodeSets[0];
		fw = removableNodeSets[1];
//		log.debug("bw: " + bw + " " + bwEdges);
//		log.debug("fw: " + fw + " " + fwEdges);
//		log.debug("bw targets: " + bwTargets);
//		log.debug("fw sources: " + fwSources);
//		log.debug("rem: " + m_removeNodes + ", fixed: " + fixed);
//		log.debug("neu: " + m_neutralEdgeSet);
	}

	public Set<String> getBackwardTargets() {
		return m_backwardTargets;
	}
}
