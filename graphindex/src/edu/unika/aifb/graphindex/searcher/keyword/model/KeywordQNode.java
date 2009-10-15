package edu.unika.aifb.graphindex.searcher.keyword.model;

import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.query.QNode;

public class KeywordQNode extends QNode {

	private Set<String> m_keywords;
	
	public KeywordQNode(String label) {
		super(label);
		m_keywords = new HashSet<String>();
	}
	
	public Set<String> getKeywords() {
		return m_keywords;
	}
	
	public String toString() {
		return "kw:" + m_keywords.toString();
	}

	public void addKeyword(String keyword) {
		m_keywords.add(keyword);
	}
}
