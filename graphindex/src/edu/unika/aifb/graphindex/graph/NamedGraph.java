package edu.unika.aifb.graphindex.graph;

import java.util.Set;

import org.jgrapht.EdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

/**
 * A subclass of DirectedMultigraph, which adds a name and the ability to store and load the graph
 * using the graph storage interface. Vertices and edge labels have to be strings.
 * 
 * @author gl
 *
 * @param <V> vertex type
 * @param <E> edge type
 */
public class NamedGraph<V extends String, E extends LabeledEdge<String>> extends DirectedMultigraph<String,LabeledEdge<String>> {
	
	private static final long serialVersionUID = -6948953756502811617L;
	
	private String m_name;
	private GraphStorage m_gs;
	
	public NamedGraph(String name, Class<? extends E> edgeClass) throws StorageException {
		super(edgeClass);
		m_name = name;
		initialize();
	}

	public NamedGraph(String name, EdgeFactory<String,LabeledEdge<String>> ef) throws StorageException {
		super(ef);
		m_name = name;
		initialize();
	}
	
	private void initialize() throws StorageException {
		if (StorageManager.getInstance().getGraphManager() != null) {
			m_gs = StorageManager.getInstance().getGraphManager().getGraphStorage();
			
			Set<LabeledEdge<String>> edges = m_gs.loadEdges(m_name);
			for (LabeledEdge<String> edge : edges) {
				addVertex(edge.getSrc());
				addVertex(edge.getDst());
				addEdge(edge.getSrc(), edge.getDst(), edge);
			}
		}
	}
	
	public String getName() {
		return m_name;
	}
	
	public void store() throws StorageException {
		m_gs.saveEdges(m_name, edgeSet());
	}
	
	/**
	 * Shortcut method to add an edge, without having to add the vertices beforehand.
	 * 
	 * @param src
	 * @param edge
	 * @param dst
	 */
	public void addEdge(V src, String edge, V dst) {
		addVertex(src);
		addVertex(dst);
		addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	public String toString() {
		return m_name + "(" + vertexSet().size() + "," + edgeSet().size() + ")";
	}
}
