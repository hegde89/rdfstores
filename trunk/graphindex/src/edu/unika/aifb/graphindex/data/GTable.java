package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.util.Timings;

public class GTable<T extends Comparable<T>> implements Iterable<T[]>, Cloneable {
	private int m_colCount;
	private String[] m_colNames;
	private Map<String,Integer> m_name2col;
	private List<T[]> m_rows;
	private int m_sortedCol = -1;
	
	public static Timings timings;
	private static final Logger log = Logger.getLogger(GTable.class);
	
	public GTable(int cols) {
		m_colCount = cols;
		m_rows = new ArrayList<T[]>();
		m_name2col = new HashMap<String,Integer>();
	}
	
	public GTable(List<String> colNames) {
		this(colNames.toArray(new String [colNames.size()]));
	}
	
	public GTable(List<String> colNames, int initialCapacity) {
		this(colNames.toArray(new String [colNames.size()]));
		m_rows = new ArrayList<T[]>(initialCapacity);
	}
	
	public GTable(String... colNames) {
		this(colNames.length);
		m_colNames = new String[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			m_colNames[i] = colNames[i];
			m_name2col.put(colNames[i], i);
		}
	}
	
	public GTable(GTable<T> table) {
		this(table, true);
	}

	public GTable(GTable<T> table, boolean rows) {
		this(table.getColumnNames());
		if (table.isSorted())
			setSortedColumn(table.getSortedColumn());
		if (rows)
			m_rows = table.getRows();
	}
	
	public void setRows(List<T[]> rows) {
		m_rows = rows;
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
	
	public T getValue(T[] row, String colName) {
		return row[getColumn(colName)];
	}
	
	public String getSortedColumn() {
		if (m_sortedCol == -1)
			return null;
		
		return m_colNames[m_sortedCol];
	}
	
	public void setSortedColumn(int i) {
		m_sortedCol = i;
	}
	
	public void setSortedColumn(String colName) {
		m_sortedCol = getColumn(colName);
	}
	
	public void setUnsorted() {
		m_sortedCol = -1;
	}
	
	public boolean isSorted() {
		return m_sortedCol >= 0;
	}
	
	public boolean isSortedBy(String col) {
		if (m_sortedCol < 0)
			return false;
		// getColumn returns -1 if column unknown, but not a problem here,
		// because that case is already handled above
		return m_sortedCol == getColumn(col); 
	}
	
	public boolean isSortedBy(int col) {
		if (m_sortedCol < 0)
			return false;
		// getColumn returns -1 if column unknown, but not a problem here,
		// because that case is already handled above
		return m_sortedCol == col; 
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
	
	public void addRows(List<T[]> rows) {
		m_rows.addAll(rows);
	}
	
	public T[] getRow(int row) {
		return m_rows.get(row);
	}
	
	public List<T[]> getRows() {
		return m_rows;
	}
	
	public int rowCount() {
		return m_rows.size();
	}

	public Iterator<T[]> iterator() {
		return m_rows.iterator();
	}
	
	public List<String> getColumnNamesSorted() {
		List<String> sorted = new ArrayList<String>(Arrays.asList(m_colNames));
		Collections.sort(sorted);
		return sorted;
	}
	
	public void removeDuplicates(List<String> columns) {
		List<T[]> result = new ArrayList<T[]>();
		Set<String> sigs = new HashSet<String>();
		int[] cols = new int [columns.size()];
		for (int i = 0; i < columns.size(); i++)
			cols[i] = getColumn(columns.get(i));
		for (T[] row : m_rows) {
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < cols.length; i++) 
				sb.append(row[cols[i]]).append("__");
			
			String sig = sb.toString();
			if (!sigs.contains(sig)) {
				sigs.add(sig);
				result.add(row);
			}
		}
		log.debug("purged duplicates: " + rowCount() + " => " + result.size());
		setRows(result);
	}
	
	public Set<T> getUniqueValueSet(String colName) {
		Set<T> values = new HashSet<T>();
		int col = getColumn(colName);
		for (T[] row : this)
			values.add(row[col]);
		return values;
	}
	
	public void removeDuplicates() {
		removeDuplicates(Arrays.asList(m_colNames));
	}

	/**
	 * Sort the table by a column.
	 * @param col
	 * @param conditional if true, only sort if table is unsorted on col
	 */
	public void sort(String col, boolean conditional) {
		if (conditional)
			if (!isSortedBy(col))
				sort(col);
		else
			sort(col);
	}
	
	public void sort(int col, boolean conditional) {
		if (conditional)
			if (!isSortedBy(col))
				sort(col);
		else
			sort(col);
	}

	public void sort(String col) {
		sort(getColumn(col));
	}
	
	public void sort(final int col) {
		if (timings != null)
			timings.start(Timings.TBL_SORT);
//		long start = System.currentTimeMillis();
//		String s = this.toString();
		Collections.sort(m_rows, new Comparator<T[]>() {
			public int compare(T[] r1, T[] r2) {
				return r1[col].compareTo(r2[col]);
			}
		});
		setSortedColumn(col);
//		if (log.isDebugEnabled())
//			log.debug(" sorted " + s + " by " + getColumnName(col) + " in " + (System.currentTimeMillis() - start) + " ms");
		if (timings != null)
			timings.end(Timings.TBL_SORT);
	}
	
	public void sort(final List<String> columns) {
		Collections.sort(m_rows, new Comparator<T[]>() {
			public int compare(T[] o1, T[] o2) {
				StringBuilder s1 = new StringBuilder(), s2 = new StringBuilder();
				for (String col : columns) {
					int idx = getColumn(col);
					s1.append(o1[idx]);
					s2.append(o2[idx]);
				}
				return s1.toString().compareTo(s2.toString());
			}
			
		});
		setSortedColumn(columns.get(0));
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone(); // shallow copy
	}
	
	public String toDataString() {
		return toDataString(rowCount());
	}
	
	public String toDataString(boolean printHeader) {
		return toDataString(rowCount(), printHeader);
	}
	
	public String toDataString(int rows, boolean printHeader) {
		StringBuilder sb = new StringBuilder();
		List<String> sorted = getColumnNamesSorted();
		if (printHeader) {
			for (String col : sorted) {
				sb.append(col).append("\t");
			}
			sb.append("\n");
		}
		
		for (int i = 0; i < Math.min(rows, m_rows.size()); i++) {
			for (String col : sorted)
				sb.append(m_rows.get(i)[getColumn(col)]).append("\t");
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	public String toDataString(int rows) {
		return toDataString(rows, true);
	}
	
	public String toString() {
		String s = "Table(";
		String comma = "";
		for (int i = 0; i < m_colCount; i++) {
			s += comma + m_colNames[i] + (i == m_sortedCol ? "*" : "");
			comma = ",";
		}
		return s + "|" + rowCount() + ")";
	}

	public List<T[]> getTable() {
		return m_rows;
	}

	public String getColumnName(int col) {
		return m_colNames[col];
	}
}
