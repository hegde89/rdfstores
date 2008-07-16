/**
 * 
 */
package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class ResultSet {
	String[] m_variables;
	List<Result> m_results;
	Map<String,Integer> m_var2ColIdx;
	
	public ResultSet(String[] vars) {
		m_variables = vars;
		m_results = new ArrayList<Result>();
		
		m_var2ColIdx = new HashMap<String,Integer>();
		for (int i = 0; i < m_variables.length; i++) {
			m_var2ColIdx.put(m_variables[i], i);
		}
	}

	public ResultSet(String[] vars, List<Result> data) {
		this(vars);
		m_results = data;
	}
	
	public int size() {
		return m_results.size();
	}
	
	public int getColumnForVariable(String var) {
		return m_var2ColIdx.get(var);
	}

	public Iterator<Result> iterator() {
		return m_results.iterator();
	}
	
	public Result get(int row) {
		return m_results.get(row);
	}
	
	public void addResult(Result r) {
		r.setResultSet(this);
		m_results.add(r);
	}
	
	public void addResult(Object[] data) {
		m_results.add(new Result(this, data));
	}
	
	public String[] getVars() {
		return m_variables;
	}
}