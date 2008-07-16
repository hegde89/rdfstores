package edu.unika.aifb.graphindex.extensions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.Util;


public class ExtEntry implements Serializable {
	private static final long serialVersionUID = -7380924003954574641L;
	private String m_uri;
	private String m_edgeUri;
	private List<String> m_parents;
	
	public ExtEntry(String uri, String edgeUri) {
		super();
		m_uri = uri;
		m_edgeUri = edgeUri;
		m_parents = new ArrayList<String>();
	}
	
	public String getUri() {
		return m_uri;
	}
	
	public String getEdgeUri() {
		return m_edgeUri;
	}
	
	public List<String> getParents() {
		return m_parents;
	}
	
	public void addParent(String parentUri) {
		if (!m_parents.contains(parentUri))
			m_parents.add(parentUri);
	}
	
	public String toString() {
		return "(" + Util.truncateUri(m_uri) + "|" + Util.truncateUri(m_edgeUri) + " " + m_parents + ")";
	}
}
