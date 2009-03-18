package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.unika.aifb.graphindex.data.GTable;

public class EvaluationClass {
	private Map<String,String> m_matches;
	private GTable<String> m_mappings;
//	private GTable<String> m_result;
	private List<GTable<String>> m_results;
	private boolean m_empty = false;
	
	public EvaluationClass(Map<String,String> matches, String[] colNames) {
		m_matches = matches;
		m_mappings = new GTable<String>(colNames);
		m_results = new ArrayList<GTable<String>>();
	}
	
	public EvaluationClass(GTable<String> mappings) {
		m_mappings = mappings;
		m_matches = new HashMap<String,String>();
		m_results = new ArrayList<GTable<String>>();
	}
	
	public void setEmpty(boolean empty) {
		m_empty = empty;
	}
	
	public boolean isEmpty() {
		return m_empty;
	}
	
	public void addMapping(String[] mapping) {
		m_mappings.addRow(mapping);
	}
	
	public String getMatch(String key) {
		return m_matches.get(key);
	}
	
	public List<GTable<String>> getResults() {
		return m_results;
	}
	
	public void setResults(List<GTable<String>> results) {
		m_results = results;
	}
	
	public GTable<String> findResult(String column) {
		for (GTable<String> result : m_results)
			if (result.hasColumn(column))
				return result;
		return null;
	}
	
//	public GTable<String> getResult() {
//		return m_result;
//	}
//
//	public void setResult(GTable<String> result) {
//		m_result = result;
//	}
	
	public String getMinUniqueValuesKey() {
		String maxKey = null;
		int maxCount = 0;
		
		for (String key : m_mappings.getColumnNames()) {
			Map<String,Integer> val2count = new HashMap<String,Integer>();
			for (String[] map : m_mappings) {
				String val = m_mappings.getValue(map, key);
				if (!val2count.containsKey(val))
					val2count.put(val, 1);
				else
					val2count.put(val, val2count.get(val) + 1);
			}
			for (String val : val2count.keySet()) {
				if (val2count.get(val) > maxCount) {
					maxKey = key;
					maxCount = val2count.get(val);
				}
			}
		}
		
		return maxKey;
	}
	
	public Map<String,Integer> getCardinalityMap() {
		Map<String,Integer> cardinality = new HashMap<String,Integer>();
		for (String key : m_mappings.getColumnNames()) {
			Set<String> values = new HashSet<String>();
			for (String[] map : m_mappings)
				values.add(m_mappings.getValue(map, key));
			cardinality.put(key, values.size());
		}
		return cardinality;
	}

	public List<EvaluationClass> addMatch(String key) {
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		Map<String,EvaluationClass> val2class = new HashMap<String,EvaluationClass>();

		int j = 0;
		GTable<String> mappings = m_mappings;
		m_mappings = new GTable<String>(mappings.getColumnNames());
		for (String[] map : mappings) {
			String val = mappings.getValue(map, key);
			if (val2class.size() == 0) {
				// first value is added to this class, others to new classes
				m_matches.put(key, val);
				val2class.put(val, this);
				m_mappings.addRow(map);
			}
			else {
				EvaluationClass ec = val2class.get(val);
//				if (ec == this) // to avoid concurrent modification exception
//					continue;
				
				if (ec == null) {
					Map<String,String> newMatches = new HashMap<String,String>(m_matches);
					newMatches.put(key, val);
					ec = new EvaluationClass(newMatches, mappings.getColumnNames());
					ec.setResults(new ArrayList<GTable<String>>(getResults()));
					newClasses.add(ec);
					val2class.put(val, ec);
				}
				
				ec.addMapping(map);
			}
			
//			j++;
//			if (j % 10000 == 0) {
//				System.out.println(j + " " + val2class.keySet().size() + " " + newClasses.size());
//			}
		}
		
		return newClasses;
	}
	
	public String toString() {
		return "[" + m_matches.toString() + ", " + m_mappings.toString() + "]";// + ", mappings: " + m_mappings;
	}
}