package edu.unika.aifb.graphindex.test;


import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.extensions.ExtEntry;
import edu.unika.aifb.graphindex.extensions.Extension;
import edu.unika.aifb.graphindex.extensions.ExtensionManager;

public class ExtensionManagerTest {

	private Extension extension;
	private ExtensionManager extManager = ExtensionManager.getInstance();
	
	@Before
	public void setUp() throws Exception {
		extension = new Extension("http://example.org/example-ext");
		
//		ExtEntry e1 = new ExtEntry("http://example.org/uri1");
//		e1.addParent("http://example.org/parent1");
//	
//		ExtEntry e2 = new ExtEntry("http://example.org/uri2");
//		e2.addParent("http://example.org/parent2");
//		
//		extension.add("http://example.org/edge1", e1);
//		extension.add("http://example.org/edge2", e2);
	}
	
	@Test
	public void testUnloadAll() {
		extManager.addExtension(extension);
		extManager.unloadAllExtensions();
	}
	
	@Test
	public void testGetNewExtension() {
		Extension ext = extManager.getExtension("http://example.org/ext3");
		assertNotNull(ext);
	}
	
	@Test
	public void testLoadExtension() {
		extManager.addExtension(extension);
		extManager.unloadAllExtensions();
		assertFalse(extManager.isExtensionLoaded("http://example.org/example-ext"));
		
		Extension ext = extManager.getExtension("http://example.org/example-ext");
		assertTrue(extManager.isExtensionLoaded("http://example.org/example-ext"));
		assertEquals(ext.getEdgeUris(), extension.getEdgeUris());
	}
}
