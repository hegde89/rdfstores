package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.unika.aifb.graphindex.algorithm.DGMerger;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphFactory;


public class DGMergerTest {

	@Test
	public void testSubsumes() {
		Graph g1 = GraphFactory.graph();
		Graph g2 = GraphFactory.graph();
		
		g1.addEdge("http://example.org/dataguide#O18352", "classifiedByTopic", "http://example.org/dataguide#O18376");
		g1.addEdge("http://example.org/dataguide#O18371", "hyponymOf", "http://example.org/dataguide#O18372");
		g1.addEdge("http://example.org/dataguide#O18356", "hyponymOf", "http://example.org/dataguide#O18357");
		g1.addEdge("http://example.org/dataguide#O18358", "hyponymOf", "http://example.org/dataguide#O18359");
		g1.addEdge("http://example.org/dataguide#O18352", "hyponymOf", "http://example.org/dataguide#O18353");
		g1.addEdge("http://example.org/dataguide#O18354", "hyponymOf", "http://example.org/dataguide#O18355");
		g1.addEdge("http://example.org/dataguide#O18379", "classifiedByTopic", "http://example.org/dataguide#O18383");
		g1.addEdge("http://example.org/dataguide#O18373", "hyponymOf", "http://example.org/dataguide#O18374");
		g1.addEdge("http://example.org/dataguide#O18359", "hyponymOf", "http://example.org/dataguide#O18360");
		g1.addEdge("http://example.org/dataguide#O18376", "classifiedByUsage", "http://example.org/dataguide#O18378");
		g1.addEdge("http://example.org/dataguide#O18369", "hyponymOf", "http://example.org/dataguide#O18370");
		g1.addEdge("http://example.org/dataguide#O18364", "hyponymOf", "http://example.org/dataguide#O18365");
		g1.addEdge("http://example.org/dataguide#O18387", "hyponymOf", "http://example.org/dataguide#O18371");
		g1.addEdge("http://example.org/dataguide#O18365", "hyponymOf", "http://example.org/dataguide#O18366");
		g1.addEdge("http://example.org/dataguide#O18363", "hyponymOf", "http://example.org/dataguide#O18364");
		g1.addEdge("http://example.org/dataguide#O18362", "hyponymOf", "http://example.org/dataguide#O18363");
		g1.addEdge("http://example.org/dataguide#O18361", "hyponymOf", "http://example.org/dataguide#O18362");
		g1.addEdge("http://example.org/dataguide#O18361", "classifiedByTopic", "http://example.org/dataguide#O18368");
		g1.addEdge("http://example.org/dataguide#O18366", "hyponymOf", "http://example.org/dataguide#O18367");
		g1.addEdge("http://example.org/dataguide#O18368", "hyponymOf", "http://example.org/dataguide#O18369");
		g1.addEdge("http://example.org/dataguide#O18377", "hyponymOf", "http://example.org/dataguide#O18362");
		g1.addEdge("http://example.org/dataguide#O18372", "hyponymOf", "http://example.org/dataguide#O18373");
		g1.addEdge("http://example.org/dataguide#O18355", "hyponymOf", "http://example.org/dataguide#O18356");
		g1.addEdge("http://example.org/dataguide#O18370", "hyponymOf", "http://example.org/dataguide#O18371");
		g1.addEdge("http://example.org/dataguide#O18353", "hyponymOf", "http://example.org/dataguide#O18354");
		g1.addEdge("http://example.org/dataguide#O18385", "classifiedByUsage", "http://example.org/dataguide#O18378");
		g1.addEdge("http://example.org/dataguide#O18376", "hyponymOf", "http://example.org/dataguide#O18377");
		g1.addEdge("http://example.org/dataguide#O18379", "hyponymOf", "http://example.org/dataguide#O18380");
		g1.addEdge("http://example.org/dataguide#O18374", "hyponymOf", "http://example.org/dataguide#O18375");
		g1.addEdge("http://example.org/dataguide#O18357", "hyponymOf", "http://example.org/dataguide#O18358");
		g1.addEdge("http://example.org/dataguide#O18378", "hyponymOf", "http://example.org/dataguide#O18379");
		g1.addEdge("http://example.org/dataguide#R18351", "hyponymOf", "http://example.org/dataguide#O18352");
		g1.addEdge("http://example.org/dataguide#O18368", "classifiedByTopic", "http://example.org/dataguide#O18361");
		g1.addEdge("http://example.org/dataguide#O18381", "hyponymOf", "http://example.org/dataguide#O18382");
		g1.addEdge("http://example.org/dataguide#O18383", "hyponymOf", "http://example.org/dataguide#O18384");
		g1.addEdge("http://example.org/dataguide#O18380", "hyponymOf", "http://example.org/dataguide#O18381");
		g1.addEdge("http://example.org/dataguide#O18384", "hyponymOf", "http://example.org/dataguide#O18363");
		g1.addEdge("http://example.org/dataguide#O18386", "hyponymOf", "http://example.org/dataguide#O18387");
		g1.addEdge("http://example.org/dataguide#O18385", "hyponymOf", "http://example.org/dataguide#O18386");
		g1.addEdge("http://example.org/dataguide#O18353", "classifiedByTopic", "http://example.org/dataguide#O18361");
		g1.addEdge("http://example.org/dataguide#O18376", "classifiedByTopic", "http://example.org/dataguide#O18385");
		g1.setRoot("http://example.org/dataguide#R18351");
		
		g2.addEdge("http://example.org/dataguide#O321167", "hyponymOf", "http://example.org/dataguide#O321153");
		g2.addEdge("http://example.org/dataguide#O321164", "hyponymOf", "http://example.org/dataguide#O321165");
		g2.addEdge("http://example.org/dataguide#O321163", "hyponymOf", "http://example.org/dataguide#O321164");
		g2.addEdge("http://example.org/dataguide#O321160", "hyponymOf", "http://example.org/dataguide#O321161");
		g2.addEdge("http://example.org/dataguide#O321162", "hyponymOf", "http://example.org/dataguide#O321163");
		g2.addEdge("http://example.org/dataguide#O321161", "hyponymOf", "http://example.org/dataguide#O321162");
		g2.addEdge("http://example.org/dataguide#O321159", "hyponymOf", "http://example.org/dataguide#O321160");
		g2.addEdge("http://example.org/dataguide#O321166", "classifiedByTopic", "http://example.org/dataguide#O321159");
		g2.addEdge("http://example.org/dataguide#O321156", "hyponymOf", "http://example.org/dataguide#O321157");
		g2.addEdge("http://example.org/dataguide#R321148", "hyponymOf", "http://example.org/dataguide#O321149");
		g2.addEdge("http://example.org/dataguide#O321157", "hyponymOf", "http://example.org/dataguide#O321158");
		g2.addEdge("http://example.org/dataguide#O321153", "hyponymOf", "http://example.org/dataguide#O321154");
		g2.addEdge("http://example.org/dataguide#O321155", "hyponymOf", "http://example.org/dataguide#O321156");
		g2.addEdge("http://example.org/dataguide#O321154", "hyponymOf", "http://example.org/dataguide#O321155");
		g2.addEdge("http://example.org/dataguide#O321152", "hyponymOf", "http://example.org/dataguide#O321153");
		g2.addEdge("http://example.org/dataguide#O321150", "hyponymOf", "http://example.org/dataguide#O321151");
		g2.addEdge("http://example.org/dataguide#O321151", "hyponymOf", "http://example.org/dataguide#O321152");
		g2.addEdge("http://example.org/dataguide#O321149", "hyponymOf", "http://example.org/dataguide#O321150");
		g2.addEdge("http://example.org/dataguide#O321166", "hyponymOf", "http://example.org/dataguide#O321167");
		g2.addEdge("http://example.org/dataguide#O321150", "classifiedByTopic", "http://example.org/dataguide#O321159");
		g2.addEdge("http://example.org/dataguide#O321159", "classifiedByTopic", "http://example.org/dataguide#O321166");
		g2.setRoot("http://example.org/dataguide#R321148");
		
		List<Graph> list = new ArrayList<Graph>();
		list.add(g1);
		list.add(g2);
		
		DGMerger dgm = new DGMerger(list);
		
		System.out.println(dgm.subsumes(g1, g2));
		dgm.merge();
	}
}
