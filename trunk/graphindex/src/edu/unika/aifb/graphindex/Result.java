/**
 * 
 */
package edu.unika.aifb.graphindex;

import java.util.Map;

public class Result {
	private Map<String,String> m_data;
	private ResultSet m_rs;
	
	public Result(Map<String,String> data) {
		this(null, data);
	}
	
	public Result(ResultSet rs, Map<String,String> data) {
		m_rs = rs;
		m_data = data;
	}
	
	public void setResultSet(ResultSet rs) {
		m_rs = rs;
	}
	
	public String get(String var) {
		return m_data.get(var);
	}
	
	public void set(String var, String val) {
		m_data.put(var, val);
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("[");
		String comma = "";
		for (String var : m_rs.getVars()) {
			s.append(comma).append(get(var));
			comma = ",";
		}
		return s.append("]").toString();
	}
}