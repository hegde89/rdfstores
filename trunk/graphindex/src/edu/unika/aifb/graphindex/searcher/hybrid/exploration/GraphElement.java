package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

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

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public abstract class GraphElement {
	protected String m_label;
	protected Map<String,List<Cursor>> m_keywordCursors;
	protected Set<String> m_keywords;
	protected double m_cost;
	
	public GraphElement(String label) {
		m_label = label;
		m_keywordCursors = new HashMap<String,List<Cursor>>();
		m_keywords = new HashSet<String>();
	}
	
	public void reset() {
		m_keywordCursors = new HashMap<String,List<Cursor>>();
		m_keywords = new HashSet<String>();
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public void setCost(double cost) {
		m_cost = cost;
	}
	
	public abstract double getCost();
	
	public void addCursor(Cursor c) {
		boolean equalCursorPresent = false;
		
		for (KeywordSegment ks : c.getKeywordSegments()) {
			for (String keyword : ks.getAllKeywords()) {
				List<Cursor> cursors = m_keywordCursors.get(keyword);
				if (cursors == null) {
					cursors = new ArrayList<Cursor>();
					m_keywordCursors.put(keyword, cursors);
				}
				else {
					for (Cursor cursor : cursors) {
						if (cursor.getStartCursor().getKeywordSegments().equals(c.getKeywordSegments()))
							equalCursorPresent = true;
					}
				}
				cursors.add(c);
				Collections.sort(cursors);
			}
		}
		
		if (equalCursorPresent)
			c.setFinished(true);
	}
	
	public Set<List<Cursor>> getCursorCombinations(Set<String> completeKeywords) {
		Set<List<Cursor>> list = new HashSet<List<Cursor>>();
		
		int[] idx = new int [m_keywordCursors.size()];
		int[] guard = new int [m_keywordCursors.size()];
		List<String> keywords = new ArrayList<String>(m_keywordCursors.keySet());
		
		for (int i = 0; i < keywords.size(); i++) {
			idx[i] = -1;
			guard[i] = m_keywordCursors.get(keywords.get(i)).size();
		}
		
		boolean carry = true;
		do {
			List<Cursor> combination = new ArrayList<Cursor>();
			Set<String> startLabels = new HashSet<String>();
			boolean invalid = false;
			for (int i = 0; i < keywords.size(); i++) {
				if (idx[i] == -1)
					continue;
				
				Cursor c = m_keywordCursors.get(keywords.get(i)).get(idx[i]);
				Cursor startCursor = c.getStartCursor();
				if (!startLabels.add(startCursor.getGraphElement().getLabel())) {
					invalid = true;
					break;
				}
				
				combination.add(c);
			}
			
			if (!invalid) {
				Set<String> combinationKeywords = new HashSet<String>();
				Set<String> keywordElements = new HashSet<String>();
				for (Cursor c : combination) {
					for (KeywordSegment ks : c.getKeywordSegments())
						combinationKeywords.addAll(ks.getKeywords());
					
					// find the first edge in the cursor chain
					Cursor cur = c.getParent();
					EdgeElement last = null;
					while (cur != null) {
						last = (EdgeElement)cur.getGraphElement();
						cur = cur.getParent().getParent();
					}
					
					if (!keywordElements.add(last.getSource().getLabel())) {
						invalid = true;
						break;
					}
				}
				
				if (!completeKeywords.equals(combinationKeywords))
					invalid = true;
			}
			
			if (!invalid && combination.size() > 0)
				list.add(combination);
			
//			if (combination.size() == 1 && !invalid)
//				System.out.println(combination);
			
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
	
//	public abstract List<GraphElement> getNeighbors(DirectedMultigraph<NodeElement,EdgeElement> graph, Cursor cursor);
	public abstract List<GraphElement> getNeighbors(Map<NodeElement,List<EdgeElement>> node2edges, Cursor cursor);
	
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
