package edu.unika.aifb.graphindex.data;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.Iterator;

/**
 * PeekIterator wraps another iterator to provide a peek method, which returns
 * the next item in the iterator without advancing the iterator.
 * 
 * @author gla
 */
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
		throw new UnsupportedOperationException("remove not supported");
	}

	public E peek() {
		if (m_peek == null)
			m_peek = m_iter.next();
		return m_peek;
	}
}
