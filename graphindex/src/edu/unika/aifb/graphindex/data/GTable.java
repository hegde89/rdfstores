package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GTable<T> implements Iterable<T[]>, Cloneable {
	private int m_colCount;
	private String[] m_colNames;
	private Map<String,Integer> m_name2col;
	private List<T[]> m_rows;
	
	public GTable(int cols) {
		m_colCount = cols;
		m_rows = new ArrayList<T[]>();
		m_name2col = new HashMap<String,Integer>();
	}
	
	public GTable(List<String> colNames) {
		this(colNames.toArray(new String [colNames.size()]));
	}
	
	public GTable(String... colNames) {
		this(colNames.length);
		m_colNames = colNames;
		for (int i = 0; i < colNames.length; i++)
			m_name2col.put(colNames[i], i);
	}
	
	public void setColumnName(int col, String name) {
		m_colNames[col] = name;
		m_name2col.clear();
		for (int i = 0; i < m_colNames.length; i++)
			m_name2col.put(m_colNames[i], i);
	}
	
	public boolean hasColumn(String col) {
		return m_name2col.containsKey(col);
	}
	
	public String[] getColumnNames() {
		return m_colNames;
	}
	
	public int getColumn(String colName) {
		try {
		int col = m_name2col.get(colName);
		return col;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public int columnCount() {
		return m_colCount;
	}
	
	public void addRow(T[] row) {
		m_rows.add(row);
	}
	
	public T[] getRow(int row) {
		return m_rows.get(row);
	}
	
	public int rowCount() {
		return m_rows.size();
	}

	public Iterator<T[]> iterator() {
		return m_rows.iterator();
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone(); // shallow copy
	}
	
	public String toDataString() {
		StringBuilder sb = new StringBuilder();
		for (String col : m_colNames) {
			sb.append(col).append("\t");
		}
		sb.append("\n");
		
		for (T[] row : m_rows) {
			for (T val : row)
				sb.append(val).append("\t");
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	public String toString() {
		String s = "Table(";
		String comma = "";
		for (String colName : m_colNames) {
			s += comma + colName;
			comma = ",";
		}
		return s + ")";
	}

	public List<T[]> getTable() {
		return m_rows;
	}
}
