package edu.unika.aifb.graphindex.algorithm.largercp;

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