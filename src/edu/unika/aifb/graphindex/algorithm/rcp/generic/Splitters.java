package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.LinkedList;


public class Splitters<V,E> {
	LinkedList<XBlock<V,E>> m_splitters;
	
	public Splitters() {
		m_splitters = new LinkedList<XBlock<V,E>>();
	}
	
	public void add(XBlock<V,E> xblock) {
		m_splitters.addLast(xblock);
	}
	
	public boolean contains(XBlock<V,E> xblock) {
		return m_splitters.contains(xblock);
	}
	
	public XBlock<V,E>remove() {
		return m_splitters.removeFirst();
	}
	
	public int size() {
		return m_splitters.size();
	}
	
	public String toString() {
		String s = "";
		String comma = "";
		for (XBlock<V,E> b : m_splitters) {
			s += comma + b;
			comma = ", ";
		}
		return s;
	}
}