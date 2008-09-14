package edu.unika.aifb.graphindex;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashValueProvider {
	private Map<Long,String> m_hashes;
	private Map<Long,String> m_edgeHashes;
	private BufferedReader m_in;
	
	public HashValueProvider(String hashFile) throws FileNotFoundException {
		m_in = new BufferedReader(new FileReader(hashFile));
		m_edgeHashes = new HashMap<Long,String>();
		m_hashes = new HashMap<Long,String>();
	}
	
	public void setEdges(Set<String> edges) {
		m_edgeHashes.clear();
		for (String edge : edges) {
			m_edgeHashes.put(Util.hash(edge), edge);
		}
	}
	
	public Set<Long> getEdges() {
		return m_edgeHashes.keySet();
	}
	
	public String getValue(long hash) {
		String value = m_edgeHashes.get(hash);
		if (value != null)
			return value;
		
		value = m_hashes.get(hash);
		if (value != null)
			return value;
		
		String input;
		try {
			while ((input = m_in.readLine()) != null) {
				int idx = input.indexOf("\t");
				if (idx < 0)
					System.out.println(input);
				long h = Long.parseLong(input.substring(0, idx));
				value = input.substring(idx + 1);
				if (!m_hashes.containsKey(h))
					m_hashes.put(h, value);
				if (h == hash)
					break;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
		return m_hashes.get(hash);
	}
}
