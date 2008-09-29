package edu.unika.aifb.graphindex.graph.isomorphism;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

/**
 * This feasibility checker for the vf2 algorithm checks if mapped edges have the same label.
 * 
 * @author gl
 *
 */
public class EdgeLabelFeasibilityChecker implements FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>> {

	private Set<String> m_invalidVertices;
	
	public EdgeLabelFeasibilityChecker() {
		m_invalidVertices = new HashSet<String>();
	}
	
	public EdgeLabelFeasibilityChecker(Set<String> vertices) {
		m_invalidVertices =  vertices;
	}

	public boolean isEdgeCompatible(LabeledEdge<String> e1, LabeledEdge<String> e2) {
//		System.out.println(e1 + " " + e2 + " " + e1.getLabel() + " " + e2.getLabel());
		return e1.getLabel().equals(e2.getLabel());
	}

	public boolean isVertexCompatible(String n1, String n2) {
//		if (m_invalidVertices.size() > 0)
//			System.out.println(m_invalidVertices + " " + n1 + "__" + n2);
		if (m_invalidVertices.contains(n1 + "__" + n2)) {
//			System.out.println("false");
			return false;
		}
		return true;
	}

}
