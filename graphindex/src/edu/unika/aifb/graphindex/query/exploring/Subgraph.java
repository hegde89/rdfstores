package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.equivalence.EquivalenceComparator;
import org.jgrapht.experimental.isomorphism.GraphIsomorphismInspector;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.keywordsearch.KeywordSegement;

import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;;

public class Subgraph extends DefaultDirectedGraph<NodeElement,EdgeElement> implements Comparable<Subgraph> {
	private static final long serialVersionUID = -5730502189634789126L;
	
	private Set<Cursor> m_cursors;
	private Set<EdgeElement> m_edges;
	private int m_cost;
	private Map<NodeElement,Set<String>> m_startElements;
	private Map<String,String> m_labels;
	private Map<String,String> m_vars;
	private List<String> m_selectVariables;

	private HashMap<String,Set<KeywordSegement>> m_select2ks;
	
	private static final Logger log = Logger.getLogger(Subgraph.class);
	
	public Subgraph(Class<? extends EdgeElement> arg0) {
		super(arg0);
		
		m_edges = new HashSet<EdgeElement>();
	}

	public Subgraph(Set<Cursor> cursors) {
		this(EdgeElement.class);
		
		m_cursors = cursors;
		for (Cursor c : cursors) {
			if (c.getCost() > m_cost)
				m_cost = c.getCost();
			for (GraphElement e : c.getPath()) {
				if (e instanceof EdgeElement) {
					m_edges.add((EdgeElement)e);
					addVertex(((EdgeElement)e).getSource());
					addVertex(((EdgeElement)e).getTarget());
					addEdge(((EdgeElement)e).getSource(), ((EdgeElement)e).getTarget(), (EdgeElement)e);
				}
			}
		}

		NodeElement start = null;
		for (NodeElement node : vertexSet()) {
			if (inDegreeOf(node) + outDegreeOf(node) == 1) {
				start = node;
				break;
			}
		}	
		if (start != null)
			m_cost = getLongestPath(start, new HashSet<String>()) * 2;

		//		log.debug(" " + m_cost);
//		m_cost = edgeSet().size();
//		log.debug(" " + m_cost);
	}
	
	public Set<Cursor> getCursors() {
		return m_cursors;
	}
	
	private int getLongestPath(NodeElement node, Set<String> path) {
		Set<String> newPath = new HashSet<String>(path);
		newPath.add(node.getLabel());
		
		Set<NodeElement> next = new HashSet<NodeElement>();
		
		for (EdgeElement edge : outgoingEdgesOf(node)) 
			if (!path.contains(edge.getTarget().getLabel())) 
				next.add(edge.getTarget());
		for (EdgeElement edge : incomingEdgesOf(node)) 
			if (!path.contains(edge.getSource().getLabel()))
				next.add(edge.getSource());
		
		int max = 0;
		for (NodeElement nextNode : next) {
			int length = getLongestPath(nextNode, newPath);
			if (length > max)
				max = length;
		}
		
		return Math.max(max, path.size());
	}
	
	public int getCost() {
		return m_cost;
	}

	public int compareTo(Subgraph o) {
		return ((Integer)getCost()).compareTo(o.getCost());
	}
	
	private void generateLabelMappings() {
		m_startElements = new HashMap<NodeElement,Set<String>>();
		m_selectVariables = new ArrayList<String>();
		m_select2ks = new HashMap<String,Set<KeywordSegement>>();
		m_labels = new HashMap<String,String>();
		m_vars = new HashMap<String,String>();

		int x = 1;
		for (Cursor c : m_cursors) {
			Cursor start = c.getStartCursor();
			NodeElement startElement = (NodeElement)start.getGraphElement();

//			if (m_labels.containsKey(startElement.getLabel()))
//				continue;
			
			assert start.getKeywordSegments().size() == 1;
			KeywordSegement startKS = null;
			for (KeywordSegement ks : start.getKeywordSegments())
				startKS = ks;
			
			if (!m_startElements.containsKey(startElement)) {
				String var = "?x" + x++;
				m_startElements.put(startElement, new HashSet<String>(startKS.getKeywords()));
				m_labels.put(startElement.getLabel(), var);
				m_vars.put(var, startElement.getLabel());
				m_selectVariables.add(var);
				m_select2ks.put(var, new HashSet<KeywordSegement>(Arrays.asList(startKS)));
			}
			else {
				m_startElements.get(startElement).addAll(startKS.getKeywords());
				m_select2ks.get(m_labels.get(startElement.getLabel())).add(startKS);
			}
			
//			if (!start.isFakeStart() && c.getStartElement() instanceof NodeElement) {
//				m_startElements.put((NodeElement)c.getStartElement(), c.getKeywordSegments()());
//				if (!m_labels.containsKey(c.getStartElement().getLabel())) {
//					m_labels.put(c.getStartElement().getLabel(), "?x" + x++);
//					m_vars.put(m_labels.get(c.getStartElement().getLabel()), c.getStartElement().getLabel());
//					m_selectVariables.add(m_labels.get(c.getStartElement().getLabel()));
//					m_select2ks.put(m_labels.get(c.getStartElement().getLabel()), c.getKeyword());
//				}
//			}
		}

		for (EdgeElement e : m_edges) {
			String src = m_labels.get(e.getSource().getLabel());
			if (src == null) {
				src = "?x" + x++;
				m_labels.put(e.getSource().getLabel(), src);
				m_vars.put(src, e.getSource().getLabel());
			}

			String dst = m_labels.get(e.getTarget().getLabel());
			if (dst == null) {
				dst = "?x" + x++;
				m_labels.put(e.getTarget().getLabel(), dst);
				m_vars.put(dst, e.getTarget().getLabel());
			}
		}
	}
	
	public List<String> getSelectVariables() {
		if (m_selectVariables == null)
			generateLabelMappings();
		return m_selectVariables;
	}
	
	public Query toQuery(boolean withAttributes) {
		if (m_selectVariables == null)
			generateLabelMappings();
		
		Query q = new Query(null);
		q.setSelectVariables(m_selectVariables);
		
		for (EdgeElement e : m_edges) {
			String src = m_labels.get(e.getSource().getLabel());
			String dst = m_labels.get(e.getTarget().getLabel());
			q.addLiteral(new Literal(new Predicate(e.getLabel()), new Variable(src), new Variable(dst)));
		}
		
		if (withAttributes) {
			int x = 0;
			for (NodeElement startElement : m_startElements.keySet()) {
				for (String keyword : m_startElements.get(startElement))
					q.addLiteral(new Literal(new Predicate("???" + x++), new Variable(m_labels.get(startElement.getLabel())), new Constant(keyword)));
			}
		}
		
//		log.debug(q);
		
		return q;
	}
	
	public Map<String,String> getLabels() {
		if (m_labels == null)
			generateLabelMappings();
		return m_labels;
	}
	

	public List<String> getQueryNodes() {
		if (m_selectVariables == null)
			generateLabelMappings();
		
		List<String> nodes = new ArrayList<String>();
		for (String label : m_labels.values())
			if (!nodes.contains(label))
				nodes.add(label);
		return nodes;
	}
	
	public Map<String,String> getVariableMapping() {
		if (m_vars == null)
			generateLabelMappings();
		return m_vars;
	}
	
	public HashMap<String,Set<KeywordSegement>> getKSMapping() {
		if (m_vars == null)
			generateLabelMappings();
		return m_select2ks;
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String,String>> isIsomorphicTo(Subgraph sg) {
		GraphIsomorphismInspector<IsomorphismRelation> t =  AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(this, sg, 
			new EquivalenceComparator() {

				public boolean equivalenceCompare(Object arg0, Object arg1, Object arg2, Object arg3) {
					return true;
				}

				public int equivalenceHashcode(Object arg0, Object arg1) {
					return 0;
				}

		}, 
		new EquivalenceComparator() {

			public boolean equivalenceCompare(Object arg0, Object arg1, Object arg2, Object arg3) {
				return ((EdgeElement)arg0).getLabel().equals(((EdgeElement)arg1).getLabel());
			}

			public int equivalenceHashcode(Object arg0, Object arg1) {
				return arg0.hashCode();
			}
		});
		Set<String> fixedNodes = new HashSet<String>();
		for (String selectNode : m_selectVariables)
			fixedNodes.add(m_vars.get(selectNode));
		List<Map<String,String>> mappings = new ArrayList<Map<String,String>>();
		
		if (sg.vertexSet().size() == vertexSet().size() && sg.edgeSet().size() == edgeSet().size() && t.isIsomorphic()) {
			while (t.hasNext()) {
				IsomorphismRelation rel = t.next();

				HashMap<String,String> mapping = new HashMap<String,String>();
				for (NodeElement v1 : vertexSet()) {
					mapping.put(v1.getLabel(), ((NodeElement)rel.getVertexCorrespondence(v1, true)).getLabel());
				}
				mappings.add(mapping);
			}
		}
			
		return mappings;
	}
	
	public String toString() {
		return "subgraph size: " + m_edges.size() + ", cost: " + m_cost + ", " + m_edges;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_edges == null) ? 0 : m_edges.hashCode());
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
		Subgraph other = (Subgraph)obj;
		if (m_edges == null) {
			if (other.m_edges != null)
				return false;
		} else if (!m_edges.equals(other.m_edges))
			return false;
		return true;
	}

	public boolean hasDanglingEdge() {
		if (m_selectVariables == null)
			generateLabelMappings();
		
		for (NodeElement node : vertexSet()) {
			if (outDegreeOf(node) + inDegreeOf(node) == 1 && !m_selectVariables.contains(m_labels.get(node.getLabel())))
				return true;
		}
		
		return false;
	}
}
