/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.LinkedList;


public class Splitters {
	LinkedList<XBlock> m_splitters;
	
	public Splitters() {
		m_splitters = new LinkedList<XBlock>();
	}
	
	public void add(XBlock xblock) {
		m_splitters.addLast(xblock);
	}
	
	public boolean contains(XBlock xblock) {
		return m_splitters.contains(xblock);
	}
	
	public XBlock remove() {
		return m_splitters.removeFirst();
	}
	
	public int size() {
		return m_splitters.size();
	}
	
	public String toString() {
		String s = "";
		String comma = "";
		for (XBlock b : m_splitters) {
			s += comma + b;
			comma = ", ";
		}
		return s;
	}
}