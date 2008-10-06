package edu.unika.aifb.graphindex.data;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Set;

public class MemoryHashValueProvider implements HashValueProvider {

	Map<Long,String> m_hashes;
	Set<Long> m_edges;
	
	public MemoryHashValueProvider(Map<Long,String> hashes, Set<Long> edges) {
		m_hashes = hashes;
		m_edges = edges;
	}
	

	public Set<Long> getEdges() {
		return m_edges;
	}

	public String getValue(long hash) {
		return m_hashes.get(hash);
	}

	public void clearCache() throws FileNotFoundException {
		
	}
}
