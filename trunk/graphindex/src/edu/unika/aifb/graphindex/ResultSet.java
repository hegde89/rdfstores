/**
 * 
 */
package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class ResultSet implements Iterable<Result> {
	String[] m_variables;
	List<Result> m_results;
	
	public ResultSet(String[] vars) {
		m_variables = vars;
		m_results = new ArrayList<Result>();
	}

	public ResultSet(String[] vars, List<Result> data) {
		this(vars);
		m_results = data;
	}
	
	public int size() {
		return m_results.size();
	}
	
	public Iterator<Result> iterator() {
		return m_results.iterator();
	}
	
	public void addResult(Result r) {
		r.setResultSet(this);
		m_results.add(r);
	}
	
	public void addResult(Map<String,String> data) {
		m_results.add(new Result(this, data));
	}
	
	public String[] getVars() {
		return m_variables;
	}
	
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (String var : m_variables)
			s.append(var).append(" ");
		s.append("\n");
		for (Result r : m_results)
			s.append(r).append("\n");
		return s.toString();
	}
}