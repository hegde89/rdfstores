package edu.unika.aifb.graphindex.preprocessing;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.TripleSink;

public class DatasetAnalyzer implements TripleSink {

	private Map<String,Integer> m_properties;
	private static final Logger log = Logger.getLogger(DatasetAnalyzer.class);
	
	public DatasetAnalyzer() {
		m_properties = new HashMap<String,Integer>();
	}

	public void triple(String s, String p, String o) {
		if (!m_properties.containsKey(p)) {
			m_properties.put(p, 1);
		}
		else
			m_properties.put(p, m_properties.get(p) + 1);
	}
	
	public void printAnalysis() {
		log.info("properties: " + m_properties.size());
		log.info("instance counts:");
		for (String p : m_properties.keySet())
			log.info("  " + p + ": " + m_properties.get(p));
	}
}
