package edu.unika.aifb.graphindex.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.Util;

public class Graph implements Serializable, Storable {
	private static final long serialVersionUID = 6678580842722678188L;
	private String m_name;
	private int m_id;
	private String m_root;
	private Set<Vertex> m_vertices;
	private Set<Edge> m_edges;
	private Map<Vertex,Set<Edge>> m_outgoing;
	private Map<Vertex,Set<Edge>> m_incoming;
	private Map<String,Vertex> m_label2Vertex;
	private Map<String,List<Edge>> m_label2Edges;
	private GraphManager m_gm = GraphManager.getInstance();
	private int m_verticesSize = -1;
	
	private int m_status = Storable.STATUS_NEW;
	
	public Graph(String name) {
		this(name, -1, true);
	}
	
	public Graph(String name, boolean tryStub) {
		this(name, -1, tryStub);
	}
	
	public Graph(String name, int id, boolean tryStub) {
		m_id = id;
		m_name = name;
		reset();
		if (tryStub)
			if (m_gm.loadStub(this))
				m_status = Storable.STATUS_STUB;
	}
	
	public Graph(String name, int id) {
		this(name, id, true);
	}
	
	private void reset() {
		m_vertices = new HashSet<Vertex>();
		m_edges = new HashSet<Edge>();
		m_outgoing = new HashMap<Vertex,Set<Edge>>();
		m_incoming = new HashMap<Vertex,Set<Edge>>();
		m_label2Edges = new HashMap<String,List<Edge>>();
		m_label2Vertex = new HashMap<String,Vertex>();
	}
	
	private void empty() {
		m_vertices = null;
		m_edges = null;
		m_outgoing = null;
		m_incoming = null;
		m_label2Edges = null;
		m_label2Vertex = null;
	}
	
	public void setName(String name) {
		m_name = name;
	}
	
	public int getId() {
		return m_id;
	}
	
	public void setId(int id) {
		m_id = id;
	}
	
	public String getName() {
		return m_name;
	}
	
	public Vertex getRoot() {
		return m_label2Vertex.get(m_root);
	}
	
	public void setRoot(Vertex v) {
		setRoot(v.getLabel());
	}
	
	public void setRoot(String label) {
		m_root = label;
	}
	
	protected void addIncoming(Vertex v, Edge e) {
		Set<Edge> incoming = m_incoming.get(v);
		if (incoming == null) {
			incoming = new HashSet<Edge>();
			m_incoming.put(v, incoming);
		}
		incoming.add(e);
	}
	
	protected void addOutgoing(Vertex v, Edge e) {
		Set<Edge> outgoing = m_outgoing.get(v);
		if (outgoing == null) {
			outgoing = new HashSet<Edge>();
			m_outgoing.put(v, outgoing);
		}
		outgoing.add(e);
	}
	
	public boolean containsVertex(GraphElement v) {
		return m_vertices.contains(v);
	}
	
	public void addVertex(Vertex v) {
		v.setGraph(this);
		m_label2Vertex.put(v.getLabel(), v);
		m_vertices.add(v);
	}
	
	public void addVertex(String label) {
		addVertex(new Vertex(label));
	}
	
	public void addEdge(Edge e) {
		e.setGraph(this);
		m_edges.add(e);
		if (!m_label2Edges.containsKey(e.getLabel()))
			m_label2Edges.put(e.getLabel(), new ArrayList<Edge>());
		m_label2Edges.get(e.getLabel()).add(e);
		addIncoming(e.getTarget(), e);
		addOutgoing(e.getSource(), e);
	}
	
	public void addEdge(String source, String label, String target) {
		Vertex s = m_label2Vertex.get(source);
		Vertex t = m_label2Vertex.get(target);
		
		if (s == null) {
			s = new Vertex(source);
			addVertex(s);
		}
		
		if (t == null) {
			t = new Vertex(target);
			addVertex(t);
		}
		
		addEdge(new Edge(s, t, label));
	}

	public Set<Edge> outgoingEdges(Vertex vertex) {
		return m_outgoing.get(vertex) != null ? m_outgoing.get(vertex) : new HashSet<Edge>();
	}

	public Set<Edge> incomingEdges(Vertex vertex) {
		return m_incoming.get(vertex) != null ? m_incoming.get(vertex) : new HashSet<Edge>();
	}

	public Set<String> outgoingEdgeLabels(Vertex vertex) {
		Set<String> labels = new HashSet<String>();
		for (Edge e : outgoingEdges(vertex))
			labels.add(e.getLabel());
		return labels;
	}

	public Set<String> incomingEdgeLabels(Vertex vertex) {
		Set<String> labels = new HashSet<String>();
		for (Edge e : incomingEdges(vertex))
			labels.add(e.getLabel());
		return labels;
	}

	public Map<String,List<Vertex>> outgoingEdgeMap(Vertex vertex) {
		Map<String,List<Vertex>> edges = new HashMap<String,List<Vertex>>();
		for (Edge e : outgoingEdges(vertex)) {
			if (!edges.containsKey(e.getLabel()))
				edges.put(e.getLabel(), new ArrayList<Vertex>());
			edges.get(e.getLabel()).add(e.getTarget());
		}
		return edges;
	}

	public Map<String,List<Vertex>> incomingEdgeMap(Vertex vertex) {
		Map<String,List<Vertex>> edges = new HashMap<String,List<Vertex>>();
		for (Edge e : incomingEdges(vertex)) {
			if (!edges.containsKey(e.getLabel()))
				edges.put(e.getLabel(), new ArrayList<Vertex>());
			edges.get(e.getLabel()).add(e.getTarget());
		}
		return edges;
	}

	public int outDegreeOf(Vertex v) {
		return m_outgoing.get(v) != null ? m_outgoing.get(v).size() : 0;
	}

	public int inDegreeOf(Vertex v) {
		return m_incoming.get(v) != null ? m_incoming.get(v).size() : 0;
	}
	
	public int numberOfVertices() {
		if (isStubbed())
			return m_verticesSize;
		else if (m_vertices != null)
			return m_vertices.size();
		else
			return -1;
	}
	
	public int numberOfEdges() {
		return m_edges.size();
	}
	
	public Set<Vertex> vertices() {
		return m_vertices;
	}
	
	public Set<Edge> edges() {
		return m_edges;
	}
	
	public Vertex getVertex(String label) {
		return m_label2Vertex.get(label);
	}
	
	public Edge getEdge(Vertex source, Vertex target, String label) {
		for (Edge e : source.outgoingEdges()) {
			if (e.getLabel().equals(label) && e.getTarget().equals(target))
				return e;
		}
		return null;
	}
	
	public List<Edge> edges(String label) {
		return m_label2Edges.get(label) != null ? m_label2Edges.get(label) : new ArrayList<Edge>();
	}
	
	public String toDot() {
		String s = "digraph \"" + m_name + "\" {\n";
		
		for (Vertex v : m_vertices) {
			s += "\t\"" + v.getLabel() + "\" [label=\"" + Util.truncateUri(v.getLabel()) + "\"];\n";
		}
		
		for (Edge e : m_edges) {
			s += "\t\"" + e.getSource().getLabel() + "\" -> \"" + e.getTarget().getLabel() + "\" [label=\"" + Util.truncateUri(e.getLabel()) + "\"];\n";
		}
		
		return s + "}\n";
	}
	
	public void setNumberOfVertices(int x) {
		m_verticesSize = x;
	}

	public int getStatus() {
		return m_status;
	}

	public void load() {
		reset();
		m_gm.readGraph(this);
		m_status = Storable.STATUS_LOADED;
	}

	public void remove() {
		empty();
		m_gm.removeGraph(this);
		m_status = Storable.STATUS_REMOVED;
	}

	public void store() {
		m_verticesSize = m_vertices.size();
		m_gm.writeGraph(this);
	}

	public void unload(boolean write) {
		if (write) {
			store();
		}
		m_verticesSize = m_vertices.size();
		empty();
		m_status = Storable.STATUS_STUB;
	}
	
	public String toString() {
		return getName() + "(" + numberOfVertices() + "," + numberOfEdges() + "," + getRoot().getLabel() + ")";
	}
	
	public void unload() {
		unload(true);
	}

	public boolean isLoaded() {
		return getStatus() == Storable.STATUS_LOADED;
	}

	public boolean isNew() {
		return getStatus() == Storable.STATUS_NEW;
	}

	public boolean isRemoved() {
		return getStatus() == Storable.STATUS_REMOVED;
	}

	public boolean isStubbed() {
		return getStatus() == Storable.STATUS_STUB;
	}
}
