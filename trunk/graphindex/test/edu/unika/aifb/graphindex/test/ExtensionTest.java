package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.extensions.ExtEntry;
import edu.unika.aifb.graphindex.extensions.Extension;

public class ExtensionTest {
	@Test
	public void testExtension() {
		Extension ext = new Extension("ext1");
		ext.add("uri1", "edge1", "p1");
		ext.add("uri1", "edge1", "p2");
		ext.add("uri2", "edge1", "p2");
		ext.add("uri2", "edge2", "p3");
		ext.add("uri3", "edge1", "p1");
		
		System.out.println(ext);
	}
}
