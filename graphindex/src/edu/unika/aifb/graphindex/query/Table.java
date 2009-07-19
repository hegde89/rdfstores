package edu.unika.aifb.graphindex.query;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Table implements Iterable<int[]> {
	private int m_colCount;
	private String[] m_colNames;
	private Map<String,Integer> m_name2col;
	private List<int[]> m_rows;
	
	public Table(int cols) {
		m_colCount = cols;
		m_rows = new ArrayList<int[]>();
		m_name2col = new HashMap<String,Integer>();
	}
	
	public Table(List<String> colNames) {
		this(colNames.toArray(new String [colNames.size()]));
	}
	
	public Table(String[] colNames) {
		this(colNames.length);
		m_colNames = colNames;
		for (int i = 0; i < colNames.length; i++)
			m_name2col.put(colNames[i], i);
	}
	
	public boolean hasColumn(String col) {
		return m_name2col.keySet().contains(col);
	}
	
	public String[] getColumnNames() {
		return m_colNames;
	}
	
	public int getColumn(String colName) {
		return m_name2col.get(colName);
	}
	
	public int columnCount() {
		return m_colCount;
	}
	
	public void addRow(int[] row) {
		m_rows.add(row);
	}
	
	public int[] getRow(int row) {
		return m_rows.get(row);
	}
	
	public int rowCount() {
		return m_rows.size();
	}

	public Iterator<int[]> iterator() {
		return m_rows.iterator();
	}
	
	public String toDataString() {
		StringBuilder sb = new StringBuilder();
		for (String col : m_colNames) {
			sb.append(col).append("\t");
		}
		sb.append("\n");
		
		for (int[] row : m_rows) {
			for (int val : row)
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
}
