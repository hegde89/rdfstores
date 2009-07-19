package edu.unika.aifb.graphindex.searcher.keyword.exploration;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegement;

public abstract class GraphElement {
	protected String m_label;
	protected Map<String,List<Cursor>> m_keywordCursors;
	protected Set<String> m_keywords;
	
	public GraphElement(String label) {
		m_label = label;
		m_keywordCursors = new HashMap<String,List<Cursor>>();
		m_keywords = new HashSet<String>();
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public void addCursor(Cursor c) {
		for (KeywordSegement ks : c.getKeywordSegments()) {
			for (String keyword : ks.getKeywords()) {
				List<Cursor> cursors = m_keywordCursors.get(keyword);
				if (cursors == null) {
					cursors = new ArrayList<Cursor>();
					m_keywordCursors.put(keyword, cursors);
				}
				cursors.add(c);
				Collections.sort(cursors);
			}
		}
	}
	
	public List<List<Cursor>> getCursorCombinations() {
		List<List<Cursor>> list = new ArrayList<List<Cursor>>();
		
		int[] idx = new int [m_keywordCursors.size()];
		int[] guard = new int [m_keywordCursors.size()];
		List<String> keywords = new ArrayList<String>(m_keywordCursors.keySet());
		
		for (int i = 0; i < keywords.size(); i++) {
			guard[i] = m_keywordCursors.get(keywords.get(i)).size();
		}
		
		boolean carry = true;
		do {
			List<Cursor> combination = new ArrayList<Cursor>();
			for (int i = 0; i < keywords.size(); i++)
				combination.add(m_keywordCursors.get(keywords.get(i)).get(idx[i]));
			list.add(combination);

			carry = true;
			for (int i = 0; i < keywords.size(); i++) {
				if (carry) {
					idx[i]++;
					carry = false;
				}
				
				if (idx[i] >= guard[i]) {
					idx[i] = 0;
					carry = true;
				}
			}
		}
		while (!carry);

		return list;
	}
	
	public abstract List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor);
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GraphElement other = (GraphElement)obj;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		return true;
	}
	
	public String toString() {
		return m_label;
	}

	public Map<String,List<Cursor>> getKeywords() {
		return m_keywordCursors;
	}
}
