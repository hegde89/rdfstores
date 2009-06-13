package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
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
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.query.model.Constant;
import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;;

public class Subgraph extends DefaultDirectedGraph<NodeElement,EdgeElement> implements Comparable<Subgraph> {
	private static final long serialVersionUID = -5730502189634789126L;
	
	private List<Cursor> m_cursors;
	private Set<EdgeElement> m_edges;
	private int m_cost;
	private Map<NodeElement,String> m_startElements;
	private Map<String,String> m_labels;
	private Map<String,String> m_vars;
	private List<String> m_selectVariables;
	
	private static final Logger log = Logger.getLogger(Subgraph.class);
	
	public Subgraph(Class<? extends EdgeElement> arg0) {
		super(arg0);
		
		m_edges = new HashSet<EdgeElement>();
	}

	public Subgraph(List<Cursor> cursors) {
		this(EdgeElement.class);
		
		m_cursors = cursors;
		for (Cursor c : cursors) {
			m_cost += c.getCost();
			for (GraphElement e : c.getPath()) {
				if (e instanceof EdgeElement) {
					m_edges.add((EdgeElement)e);
					addVertex(((EdgeElement)e).getSource());
					addVertex(((EdgeElement)e).getTarget());
					addEdge(((EdgeElement)e).getSource(), ((EdgeElement)e).getTarget(), (EdgeElement)e);
				}
			}
		}
	}
	
	public int getCost() {
		return m_cost;
	}

	public int compareTo(Subgraph o) {
		return ((Integer)getCost()).compareTo(o.getCost());
	}
	
	private void generateLabelMappings() {
		m_startElements = new HashMap<NodeElement,String>();
		m_selectVariables = new ArrayList<String>();
		m_labels = new HashMap<String,String>();
		m_vars = new HashMap<String,String>();

		int x = 1;
		for (Cursor c : m_cursors) {
			Cursor start = c.getStartCursor();
			if (!start.isFakeStart()) {
				m_startElements.put((NodeElement)c.getStartElement(), c.getKeyword());
				if (!m_labels.containsKey(c.getStartElement().getLabel())) {
					m_labels.put(c.getStartElement().getLabel(), "?x" + x++);
					m_vars.put(m_labels.get(c.getStartElement().getLabel()), c.getStartElement().getLabel());
					m_selectVariables.add(m_labels.get(c.getStartElement().getLabel()));
				}
			}
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
			for (NodeElement startElement : m_startElements.keySet()) {
				q.addLiteral(new Literal(new Predicate("???"), new Variable(m_labels.get(startElement.getLabel())), new Constant(m_startElements.get(startElement))));
			}
		}
		
//		log.debug(q);
		
		return q;
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
	
	@SuppressWarnings("unchecked")
	public Map<String,String> isIsomorphicTo(Subgraph sg) {
		GraphIsomorphismInspector<IsomorphismRelation> t =  AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(this, sg, 
			new EquivalenceComparator() {

				public boolean equivalenceCompare(Object arg0, Object arg1, Object arg2, Object arg3) {
					return true;
				}

				public int equivalenceHashcode(Object arg0, Object arg1) {
					return 0;
				}

		}, 
		new EquivalenceComparator<EdgeElement,Subgraph>() {

			public boolean equivalenceCompare(EdgeElement arg0, EdgeElement arg1, Subgraph arg2, Subgraph arg3) {
				return arg0.getLabel().equals(arg1.getLabel());
			}

			public int equivalenceHashcode(EdgeElement arg0, Subgraph arg1) {
				return arg0.hashCode();
			}
		});
		Map<String,String> mapping = new HashMap<String,String>();
		if (t.isIsomorphic()) {
			IsomorphismRelation rel = t.next();
			for (NodeElement v1 : vertexSet()) {
				mapping.put(v1.getLabel(), ((NodeElement)rel.getVertexCorrespondence(v1, true)).getLabel());
			}
		}
			
		return mapping;
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
}
