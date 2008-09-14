package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.ParanoidGraph;
import org.jgrapht.graph.SimpleGraph;
import org.junit.Test;

import edu.unika.aifb.graphindex.graph.SVertex;


public class SVertexTest {
	@Test
	public void testSVertex() {
	     SVertex v1 = new SVertex("1");
	     SVertex v2 = new SVertex("2");
	     SVertex v3 = new SVertex("1");
	     
	     System.out.println(v1.equals(v2) + " " + v1.hashCode() + " " + v2.hashCode());
	     System.out.println(v1.equals(v3) + " " + v1.hashCode() + " " + v3.hashCode());
	     System.out.println(v2.equals(v3) + " " + v2.hashCode() + " " + v3.hashCode());
	     
	     SimpleGraph<SVertex, DefaultEdge> g =
	         new SimpleGraph<SVertex, DefaultEdge>(DefaultEdge.class);
	      ParanoidGraph<SVertex, DefaultEdge> pg =
	         new ParanoidGraph<SVertex, DefaultEdge>(g);
	     pg.addVertex(v1);
	     pg.addVertex(v2);
	     try {
	         pg.addVertex(v3);
	         // should not get here
	         System.out.println("bzzt");
	     } catch (IllegalArgumentException ex) {
	         // expected, swallow
	    	 System.out.println(ex);
	     }
	}
}
