package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;

public class LuceneGraphStorageTest {

	GraphManager manager;
	
	@Before
	public void setUp() throws StorageException {
		GraphStorage gs = new LuceneGraphStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/graph/test");
		
		manager = new GraphManagerImpl();
		manager.setGraphStorage(gs);
		manager.initialize(true);

		StorageManager.getInstance().setGraphManager(manager);
	}
	
	@Test
	public void testGraphStorage() throws StorageException {
		NamedGraph<String,LabeledEdge<String>> g1 = manager.graph("test1");
		
		g1.addVertex("A");
		g1.addVertex("B");
		g1.addEdge("A", "B", new LabeledEdge<String>("A", "B", "a"));
		
		g1.store();
		g1.store();
		
		g1 = manager.graph("test1");
		
		assertEquals(2, g1.vertexSet().size());
		assertEquals(1, g1.edgeSet().size());
		assertTrue(g1.vertexSet().contains("A"));
		assertTrue(g1.vertexSet().contains("B"));
	}
	
	@After
	public void tearDown() throws StorageException {
		manager.close();
	}
}
