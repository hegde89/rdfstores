/**
 * 
 */
package edu.unika.aifb.graphindex;

public class Result {
	private Object[] m_data;
	private ResultSet m_parent;
	
	public Result(Object[] data) {
		this(null, data);
	}
	
	public Result(ResultSet parent, Object[] data) {
		m_parent = parent;
		m_data = data;
	}
	
	public void setResultSet(ResultSet rs) {
		m_parent = rs;
	}

	public Object get(int col) {
		return m_data[col];
	}
	
	public Object get(String var) {
		return m_data[m_parent.getColumnForVariable(var)];
	}
	
	public void set(String var, Object val) {
		m_data[m_parent.getColumnForVariable(var)] = val;
	}
	
	public Object[] get(String[] vars) {
		Object[] data = new Object[vars.length];
		for (int i = 0; i < vars.length; i++)
			data[i] = get(vars[i]);
		return data;
	}

	public Object[] getData() {
		return m_data;
	}

	@Override
	public String toString() {
		String s = "[";
		String comma = "";
		for (Object o : m_data) {
			s += comma + o;
			comma = ",";
		}
		return s + "]";
	}
}