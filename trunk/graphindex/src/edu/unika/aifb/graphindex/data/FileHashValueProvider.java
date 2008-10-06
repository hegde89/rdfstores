package edu.unika.aifb.graphindex.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.Util;

public class FileHashValueProvider implements HashValueProvider {
	private String m_hashFile;
	private Map<Long,String> m_hashes;
	private Map<Long,String> m_propertyHashes;
	private BufferedReader m_in;
	
	public FileHashValueProvider(String hashFile, String propertyHashFile) throws IOException {
		m_in = new BufferedReader(new FileReader(hashFile));
		m_propertyHashes = new HashMap<Long,String>();
		m_hashes = new HashMap<Long,String>();
		m_hashFile = hashFile;
		
		BufferedReader in = new BufferedReader(new FileReader(propertyHashFile));
		String input;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			String[] t = input.split("\t");
			m_propertyHashes.put(Long.parseLong(t[0]), t[1]);
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.graphindex.data.IHashValueProvider#getEdges()
	 */
	public Set<Long> getEdges() {
		return m_propertyHashes.keySet();
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.graphindex.data.IHashValueProvider#getValue(long)
	 */
	public String getValue(long hash) {
		String value = m_propertyHashes.get(hash);
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
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.graphindex.data.IHashValueProvider#clearCache()
	 */
	public void clearCache() throws FileNotFoundException {
		m_in = new BufferedReader(new FileReader(m_hashFile));
		m_hashes = new HashMap<Long,String>();
	}
}
