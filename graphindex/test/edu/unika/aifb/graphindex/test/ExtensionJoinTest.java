package edu.unika.aifb.graphindex.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.ExtensionJoin;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;


public class ExtensionJoinTest {
	ExtensionManager manager;
	
	@Before
	public void setUp() throws Exception {
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/test");
		
		manager = new LuceneExtensionManager();
		manager.setExtensionStorage(es);
		manager.initialize(true, false);
	}

	@Test
	public void testSaveData() throws StorageException, IOException {
		Set<Triple> ts1 = new HashSet<Triple>();
		ts1.add(new Triple("x", "p1", "a"));
		ts1.add(new Triple("y", "p1", "a"));
		ts1.add(new Triple("x", "p1", "b"));
		ts1.add(new Triple("x", "p2", "c"));

		Set<Triple> ts2 = new HashSet<Triple>();
		ts2.add(new Triple("f", "p3", "x"));
		ts2.add(new Triple("g", "p3", "x"));
		ts2.add(new Triple("h", "p4", "y"));
		ts2.add(new Triple("i", "p5", "y"));
		
		Extension e1 = manager.extension("e1");
		e1.addTriples(ts1);
		
		Extension e2 = manager.extension("e2");
		e2.addTriples(ts2);
		
		System.out.println(ExtensionJoin.join(e1, "p1", e2));
	}
	
	@After
	public void tearDown() throws StorageException {
		manager.close();
	}
}
