package edu.unika.aifb.graphindex.data;

import java.util.Iterator;

public class PeekIterator<E> implements Iterator<E> {

	private Iterator<E> m_iter;
	private E m_peek = null;
	
	public PeekIterator(Iterable<E> iterable) {
		m_iter = iterable.iterator();
	}
	
	public PeekIterator(Iterator<E> iter) {
		m_iter = iter;
	}
	
	public boolean hasNext() {
		if (m_peek != null)
			return true;
		return m_iter.hasNext();
	}

	public E next() {
		E next = m_peek;
		if (next == null)
			next = m_iter.next();
		m_peek = null;
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException("remove not suppored");
	}

	public E peek() {
		if (m_peek == null)
			m_peek = m_iter.next();
		return m_peek;
	}
}
