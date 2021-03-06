package edu.unika.aifb.graphindex.searcher.structured.sig;

/**
 * Copyright (C) 2009 G�nter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.unika.aifb.graphindex.data.Table;

public class EvaluationClass {
	private Map<String,String> m_matches;
	private Table<String> m_mappings;
//	private GTable<String> m_result;
	private List<Table<String>> m_results;
	private boolean m_empty = false;
	
	public EvaluationClass(Map<String,String> matches, String[] colNames) {
		m_matches = matches;
		m_mappings = new Table<String>(colNames);
		m_results = new ArrayList<Table<String>>();
	}
	
	public EvaluationClass(Table<String> mappings) {
		m_mappings = mappings;
		m_matches = new HashMap<String,String>();
		m_results = new ArrayList<Table<String>>();
	}
	
	public void setEmpty(boolean empty) {
		m_empty = empty;
	}
	
	public boolean isEmpty() {
		return m_empty;
	}
	
	public Map<String,String> getMatches() {
		return m_matches;
	}
	
	public Table<String> getMappings() {
		return m_mappings;
	}
	
	public void addMapping(String[] mapping) {
		m_mappings.addRow(mapping);
	}
	
	public String getMatch(String key) {
		return m_matches.get(key);
	}
	
	public List<Table<String>> getResults() {
		return m_results;
	}
	
	public void setResults(List<Table<String>> results) {
		m_results = results;
	}
	
	public Table<String> findResult(String column) {
		for (Table<String> result : m_results)
			if (result.hasColumn(column))
				return result;
		return null;
	}
	
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
	
	/**
	 * Returns a map of value cardinalities for all nodes and mappings in this class.
	 * 
	 * @param excludeNodes nodes to exclude (usually removed nodes)
	 * @return
	 */
	public Map<String,Integer> getCardinalityMap(Set<String> excludeNodes) {
		Map<String,Integer> cardinality = new HashMap<String,Integer>();
		for (String key : m_mappings.getColumnNames()) {
			if (excludeNodes.contains(key))
				continue;
			
			int col = m_mappings.getColumn(key);

			Set<String> values = new HashSet<String>(m_mappings.rowCount());
			for (String[] map : m_mappings)
				values.add(map[col]);
			
			cardinality.put(key, values.size());
		}
		return cardinality;
	}

	public List<EvaluationClass> addMatch(String key, boolean onlyOneClass) {
		return addMatch(key, onlyOneClass, null, null);
	}
	
	public List<EvaluationClass> addMatch(String key, boolean onlyOneClass, String valueMapNode, Map<String,List<EvaluationClass>> valueMap) {
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		Map<String,EvaluationClass> val2class = new HashMap<String,EvaluationClass>();

		Table<String> mappings = m_mappings;
		m_mappings = new Table<String>(mappings.getColumnNames());
		for (String[] map : mappings) {
			String val = mappings.getValue(map, key);
			if (val2class.size() == 0) {
				// first value is added to this class, others to new classes
				m_matches.put(key, val);
				val2class.put(val, this);
				m_mappings.addRow(map);
				if (valueMapNode != null) {
					String v = mappings.getValue(map, valueMapNode);
					if (!valueMap.containsKey(v)) {
						valueMap.put(v, new ArrayList<EvaluationClass>());
					}
					valueMap.get(v).add(this);
				}
				if (onlyOneClass) {
					m_mappings = mappings;
					return new ArrayList<EvaluationClass>();
				}
			}
			else {
				EvaluationClass ec = val2class.get(val);
//				if (ec == this) // to avoid concurrent modification exception
//					continue;
				
				if (ec == null) {
					// create a new evaluation class for this value by copying the current matches
					// (which this class and the new class have in common) and adding the new match with
					// the different value (and of course the currently examined mapping)
					Map<String,String> newMatches = new HashMap<String,String>(m_matches);
					newMatches.put(key, val);
					ec = new EvaluationClass(newMatches, mappings.getColumnNames());
					ec.setResults(new ArrayList<Table<String>>(getResults()));
					newClasses.add(ec);
					val2class.put(val, ec);
					
					if (valueMapNode != null) {
						String v = mappings.getValue(map, valueMapNode);
						if (!valueMap.containsKey(v)) {
							valueMap.put(v, new ArrayList<EvaluationClass>());
						}
						valueMap.get(v).add(ec);
					}
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
		return "[" + m_matches.toString() + ", " + m_mappings.toString() + ", " + m_results + "]";// + ", mappings: " + m_mappings;
	}
}