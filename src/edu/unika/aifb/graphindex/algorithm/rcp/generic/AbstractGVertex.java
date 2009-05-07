package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGVertex<V,E> implements GVertex<V,E> {
	private GVertex<V,E> m_next, m_prev;
	private V m_label;
	private int m_movedIn = -1;
	private Block<V,E> m_block;
	protected Set<E> m_edgeLabels;

	public AbstractGVertex(V label) {
		m_label = label;
		m_edgeLabels = new HashSet<E>();
	}
	
	public Collection<E> getEdgeLabels() {
		return m_edgeLabels;
	}
	
	public V getLabel() {
		return m_label;
	}

	public int getMovedIn() {
		return m_movedIn;
	}

	public void setMovedIn(int in) {
		m_movedIn = in;
	}

	public GVertex<V,E> getNext() {
		return m_next;
	}
	
	public GVertex<V,E> getPrev() {
		return m_prev;
	}
	
	public void setNext(GVertex<V,E> next) {
		m_next = next;
	}
	
	public void setPrev(GVertex<V,E> prev) {
		m_prev = prev;
	}
	
	public Block<V,E> getBlock() {
		return m_block;
	}
	
	public void setBlock(Block<V,E> b) {
		m_block = b;
	}
	
	public String toString() {
		return m_label.toString();
	}
}
