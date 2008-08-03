package edu.unika.aifb.graphindex.test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.storage.lucene.LuceneExtension;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.Triple;

public class TestLuceneExtensionStorage {

	ExtensionManager manager;
	
	@Before
	public void setUp() throws Exception {
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/test");
		
		manager = new LuceneExtensionManager();
		manager.setExtensionStorage(es);
		manager.initialize(true);
	}

	@Test
	public void testSaveData() throws StorageException, IOException {
		Set<Triple> ts = new HashSet<Triple>();
		
		ts.add(new Triple("s1", "p1", "o1"));
		ts.add(new Triple("s1", "p1", "o2"));
		ts.add(new Triple("s1", "p2", "o1"));
		ts.add(new Triple("s1", "p2", "o3"));
		ts.add(new Triple("s2", "p1", "o2"));
		ts.add(new Triple("s3", "p3", "o4"));
		
		System.out.println(ts);
		
		Extension e = manager.extension("ext1");
		manager.startBulkUpdate();
		e.addTriples(ts);
		manager.endBulkUpdate();
		
		Set<Triple> triples = e.getTriples("p1");
		System.out.println(triples);
	}
	
	@After
	public void tearDown() throws StorageException {
		manager.close();
	}
}
