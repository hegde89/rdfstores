package edu.unika.aifb.graphindex.test;

import org.junit.Test;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.experimental.equivalence.EquivalenceComparator;
import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;
import org.jgrapht.experimental.isomorphism.GraphIsomorphismInspector;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;

public class JGraphTTest {
	
	private void addEdge(DirectedGraph<String,LabeledEdge<String>> g, String src, String edge, String dst) {
		g.addVertex(src);
		g.addVertex(dst);
		g.addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	@Test
	public void testIsomorphism() {
		DirectedGraph<String,LabeledEdge<String>> g1 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		DirectedGraph<String,LabeledEdge<String>> g2 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		DirectedGraph<String,LabeledEdge<String>> g3 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));

		addEdge(g1, "A", "a", "B");
		addEdge(g1, "B", "b", "B");
		addEdge(g1, "B", "c", "A");
		addEdge(g1, "A", "a", "C");
		
		addEdge(g2, "A", "b", "B");

		addEdge(g3, "X", "a", "D");
		addEdge(g3, "D", "b", "D");
		addEdge(g3, "D", "c", "X");
		addEdge(g3, "X", "a", "C");
		
		EquivalenceComparator<? super String,? super Graph<String,LabeledEdge<String>>> vertexChecker = new EquivalenceComparator<String,Graph<String,LabeledEdge<String>>>() {

			public boolean equivalenceCompare(String v1, String v2, Graph<String,LabeledEdge<String>> g1, Graph<String,LabeledEdge<String>> g2) {
				DirectedGraph<String,LabeledEdge<String>> dg1 = (DirectedGraph<String,LabeledEdge<String>>)g1;
				DirectedGraph<String,LabeledEdge<String>> dg2 = (DirectedGraph<String,LabeledEdge<String>>)g2;
				
				if (dg1.inDegreeOf(v1) == dg2.inDegreeOf(v2) && dg1.outDegreeOf(v1) == dg2.outDegreeOf(v2))
					return true;
				return false;
			}

			public int equivalenceHashcode(String v, Graph<String,LabeledEdge<String>> g) {
				DirectedGraph<String,LabeledEdge<String>> dg = (DirectedGraph<String,LabeledEdge<String>>)g;
				return dg.inDegreeOf(v) + dg.outDegreeOf(v);
			}
			
		};
		
		EquivalenceComparator<LabeledEdge<String>,Graph<String,LabeledEdge<String>>> edgeChecker = new EquivalenceComparator<LabeledEdge<String>,Graph<String,LabeledEdge<String>>>() {
			public boolean equivalenceCompare(LabeledEdge<String> e1, LabeledEdge<String> e2, Graph<String,LabeledEdge<String>> g1,	Graph<String,LabeledEdge<String>> g2) {
				if (e1.getLabel().equals(e2.getLabel()))
					return true;
				return false;
			}

			public int equivalenceHashcode(LabeledEdge<String> e, Graph<String,LabeledEdge<String>> g) {
				return e.getLabel().hashCode();
			}

		};
		
		GraphIsomorphismInspector<IsomorphismRelation> x = AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(g1, g2, vertexChecker, edgeChecker);
		System.out.println(x.isIsomorphic());

		x = AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(g1, g3, vertexChecker, edgeChecker);
		System.out.println(x.isIsomorphic());
		System.out.println(x.next());
	}
}
