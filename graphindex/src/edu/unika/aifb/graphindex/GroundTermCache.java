package edu.unika.aifb.graphindex;

import java.util.HashMap;
import java.util.Map;

public class GroundTermCache {
	private Map<String,Boolean> m_cache;
	
	public GroundTermCache() {
		m_cache = new HashMap<String,Boolean>();
	}
	
	private String getCacheString(String groundTerm, String ext) {
		return new StringBuilder().append(groundTerm).append("__").append(ext).toString();
	}
	
	public void put(String groundTerm, String ext, boolean value) {
		m_cache.put(getCacheString(groundTerm, ext), value);
	}
	
	public Boolean get(String groundTerm, String ext) {
		return m_cache.get(getCacheString(groundTerm, ext));
	}

	public void clear() {
		m_cache = new HashMap<String,Boolean>();
	}

	public int size() {
		return m_cache.size();
	}
}
