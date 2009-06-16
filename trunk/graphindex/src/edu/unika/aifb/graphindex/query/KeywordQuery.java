package edu.unika.aifb.graphindex.query;

public class KeywordQuery {
	private String m_name;
	private String m_query;
	
	public KeywordQuery(String name, String query) {
		m_name = name;
		m_query = query;
	}
	
	public String getName() {
		return m_name;
	}
	
	public String getQuery() {
		return m_query;
	}
}
