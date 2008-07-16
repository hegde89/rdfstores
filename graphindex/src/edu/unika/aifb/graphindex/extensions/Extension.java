package edu.unika.aifb.graphindex.extensions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.graph.Vertex;

public class Extension implements Serializable {
	private static final long serialVersionUID = -988599444112475599L;
	private String m_uri;
	private Set<ExtEntry> m_entries;
	private Map<String,Set<ExtEntry>> m_edge2Entries;
	private Map<String,Set<ExtEntry>> m_uri2Entries;
	
	public Extension(String uri) {
		m_uri = uri;
		m_entries = new HashSet<ExtEntry>();
		m_edge2Entries = new HashMap<String,Set<ExtEntry>>();
		m_uri2Entries = new HashMap<String,Set<ExtEntry>>();
	}
	
	public String getUri() {
		return m_uri;
	}
	
	public Set<String> getEdgeUris() {
		return m_edge2Entries.keySet();
	}
	
	public Set<ExtEntry> getEntries(String edgeUri) {
		return m_edge2Entries.get(edgeUri);
	}
	
	public Set<ExtEntry> getEntries() {
		return m_entries;
	}
	
	public ExtEntry getEntry(String uri, String edgeUri) {
		Set<ExtEntry> entries = m_uri2Entries.get(uri);
		if (entries != null) {
			for (ExtEntry e : entries)
				if (e.getEdgeUri().equals(edgeUri))
					return e;
		}
		return null;
	}
	
	public Set<String> getUris() {
		return m_uri2Entries.keySet();
	}
	
	public List<String> getUris(String edgeUri) {
		List<String> list = new ArrayList<String>();
		Set<ExtEntry> entries = m_edge2Entries.get(edgeUri);
		if (entries != null) {
			for (ExtEntry e : entries)
				list.add(e.getUri());
		}
		return list;
	}
	
	public List<String> getParents(String uri, String edgeUri) {
		ExtEntry e = getEntry(uri, edgeUri);
		if (e != null)
			return e.getParents();
		return null;
	}
	
	public void add(String uri, String edgeUri, String parentUri) {
		ExtEntry e = getEntry(uri, edgeUri);
		if (e == null) {
			e = new ExtEntry(uri, edgeUri);
			add(e);
		}
		if (parentUri != null)
			e.addParent(parentUri);
	}
	
	private void add(ExtEntry e) {
		m_entries.add(e);

		if (!m_edge2Entries.containsKey(e.getEdgeUri()))
			m_edge2Entries.put(e.getEdgeUri(), new HashSet<ExtEntry>());
		m_edge2Entries.get(e.getEdgeUri()).add(e);

		if (!m_uri2Entries.containsKey(e.getUri()))
			m_uri2Entries.put(e.getUri(), new HashSet<ExtEntry>());
		m_uri2Entries.get(e.getUri()).add(e);
	}

	public int size() {
		return m_entries.size();
	}
	
	public String toString() {
		String s = "[";
		String addComma = "";
		for (ExtEntry e : m_entries) {
			s += addComma + e;
			addComma = ", ";
		}
		return s + "]";
	}

	public boolean containsUri(String uri) {
		return m_uri2Entries.keySet().contains(uri);
	}
	
	public boolean containsUri(String uri, String edgeUri) {
		return getEntry(uri, edgeUri) != null;
	}
	
	public boolean containsParent(String edgeUri, String parentUri) {
		for (ExtEntry e : getEntries(edgeUri)) {
			if (e.getParents().contains(parentUri))
				return true;
		}
		return false;
	}
	
	public Set<String> getChildren(String edgeUri, String parentUri) {
		Set<String> children = new HashSet<String>();
		for (ExtEntry e : getEntries(edgeUri)) {
			if (e.getParents().contains(parentUri))
				children.add(e.getUri());
		}
		return children;
	}
}
