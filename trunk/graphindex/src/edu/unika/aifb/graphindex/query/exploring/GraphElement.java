package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.keywordsearch.KeywordSegement;

public abstract class GraphElement {
	protected String m_label;
	protected Map<KeywordSegement,List<Cursor>> m_keywordCursors;
	
	public GraphElement(String label) {
		m_label = label;
		m_keywordCursors = new HashMap<KeywordSegement,List<Cursor>>();
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public void addCursor(Cursor c) {
		List<Cursor> cursors = m_keywordCursors.get(c.getKeyword());
		if (cursors == null) {
			cursors = new ArrayList<Cursor>();
			m_keywordCursors.put(c.getKeyword(), cursors);
		}
		cursors.add(c);
		Collections.sort(cursors);
	}
	
	public List<List<Cursor>> getCursorCombinations() {
		List<List<Cursor>> list = new ArrayList<List<Cursor>>();
		
		int[] idx = new int [m_keywordCursors.size()];
		int[] guard = new int [m_keywordCursors.size()];
		List<KeywordSegement> keywords = new ArrayList<KeywordSegement>(m_keywordCursors.keySet());
		
		for (int i = 0; i < keywords.size(); i++)
			guard[i] = m_keywordCursors.get(keywords.get(i)).size();
		
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

	public Map<KeywordSegement,List<Cursor>> getKeywords() {
		return m_keywordCursors;
	}
}
