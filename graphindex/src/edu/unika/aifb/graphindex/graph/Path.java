package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.unika.aifb.graphindex.query.Predicate;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.QueryVertex;
import edu.unika.aifb.graphindex.query.Variable;

public class Path extends QueryGraph implements Iterable<GraphElement> {
	public class PathVertexIterator<V extends Vertex> implements Iterator<V> {
		private V m_cur;
		
		public PathVertexIterator(V start) {
			m_cur = start;
		}
		
		public boolean hasNext() {
			return m_cur != null;
		}

		public V next() {
			V next = m_cur;
			m_cur = null;
			for (Edge e : next.outgoingEdges()) {
				m_cur = (V)e.getTarget();
				break;
			}
			return next;
		}

		public void remove() {
		}
	}
	
	public class PathIterator<V extends GraphElement> implements Iterator<V> {
		private V m_cur;
		
		public PathIterator(V start) {
			m_cur = start;
		}
		
		public boolean hasNext() {
			return m_cur != null;
		}

		public V next() {
			V next = m_cur;
			m_cur = null;
			if (next instanceof Vertex) {
				for (Edge e : ((Vertex)next).outgoingEdges()) {
					m_cur = (V)e;
					break;
				}
			}
			else {
				m_cur = (V)((Edge)next).getTarget();
			}
			return next;
		}

		public void remove() {
		}
	}
	
	private Vertex m_start, m_end;
	
	public Path(String name) {
		super(name);
	}
	
	public Path(String name, List<String> labels) {
		this(name);
		
		for (int i = 2; i < labels.size(); i += 2) {
			addVertex(labels.get(i - 2));
			addVertex(labels.get(i));
			addEdge(((QueryVertex)getVertex(labels.get(i - 2))).getTerm(), new Predicate(labels.get(i -1)), ((QueryVertex)getVertex(labels.get(i - 2))).getTerm());
		}
	}

	public Vertex getStartVertex() {
		return m_start;
	}
	
	public Vertex getEndVertex() {
		return m_end;
	}
	
	public int getLength() {
		return numberOfEdges();
	}

	public boolean verifyPath() {
		return true;
	}
	
	public List<String> toEdgeList() {
		List<String> list = new ArrayList<String>();
		Vertex cur = getStartVertex();
		do {
			for (Edge e : cur.outgoingEdges()) {
				list.add(e.getLabel());
				cur = e.getTarget();
				break;
			}
		}
		while (cur.outDegree() > 0);
			
		return list;
	}
	
	public List<Vertex> toVertexList() {
		List<Vertex> list = new ArrayList<Vertex>();
		Vertex cur = getStartVertex();
		do {
			for (Edge e : cur.outgoingEdges()) {
				list.add(cur);
				cur = e.getTarget();
				break;
			}
		}
		while (cur.outDegree() > 0);
		list.add(cur);	
		return list;
	}
	
	public void setStartVertex(Vertex start) {
		m_start = start;
	}

	public void setEndVertex(Vertex end) {
		m_end = end;
	}
	
	public Iterator<Vertex> vertexIterator() {
		return new PathVertexIterator<Vertex>(m_start);
	}
	
	public Iterator<GraphElement> iterator() {
		return new PathIterator<GraphElement>(m_start);
	}
	
	public String toString() {
		String s = "[";
		String addComma = "";
		for (GraphElement e : this) {
			s += addComma + e.toString();
			addComma = ", ";
		}
		return s + "]";
	}
}
