package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.List;

import org.jgrapht.graph.DirectedMultigraph;

public class NodeElement extends GraphElement {

	public NodeElement(String label) {
		super(label);
	}

	public List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor) {
		EdgeElement prevEdge = (EdgeElement)(cursor.getParent() != null ? cursor.getParent().getGraphElement() : null);

		List<GraphElement> neighbors = new ArrayList<GraphElement>();
		
		for (EdgeElement edge : graph.edgesOf(this)) {
			if (!edge.equals(prevEdge))
				neighbors.add(edge);
		}
		
		return neighbors;
	}
	
	public String toString() {
		return m_label + "[" + m_keywordCursors.size() + "]";
	}
}
