package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Matching {
	List<Map<String,String>> m_mappings;
	
	public Matching() {
		m_mappings = new ArrayList<Map<String,String>>();
	}
	
	public Matching(List<Map<String,String>> maps) {
		m_mappings = maps;
	}
	
	public void addMapping(Map<String,String> mapping) {
		m_mappings.add(mapping);
	}
	
	public List<Map<String,String>> getMappings() {
		return m_mappings;
	}

	public void addMappings(List<Map<String,String>> mappingList) {
		m_mappings.addAll(mappingList);
	}
}
