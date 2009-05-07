package edu.unika.aifb.graphindex.query.model;

public class Constant extends Term {
	private String m_value;
	private String m_typeUri;
	
	public Constant(String value) {
		m_value = value;
	}
	
	public String toString() {
		return m_value;
	}
}
