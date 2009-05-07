package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;

public class Tables {

	private static final Logger log = Logger.getLogger(Tables.class);
	public static Timings timings = null;
	
	public static GTable<String> mergeTables(List<GTable<String>> tables, final int col) {
		if (timings != null)
			timings.start(Timings.TABLEMERGE);
		long start = System.currentTimeMillis();
	
		GTable<String> result = new GTable<String>(tables.get(0).getColumnNames());
		
		if (tables.size() > 1) {
			PriorityQueue<PeekIterator<String[]>> queue = new PriorityQueue<PeekIterator<String[]>>(tables.size(), new Comparator<PeekIterator<String[]>>() {
				public int compare(PeekIterator<String[]> i1, PeekIterator<String[]> i2) {
					return i1.peek()[col].compareTo(i2.peek()[col]);
				}
			});
		
			for (GTable<String> table : tables)
				queue.add(new PeekIterator<String[]>(table.iterator()));
		
		
			while (queue.size() > 0) {
				PeekIterator<String[]> min = queue.poll();
		
				String[] row = min.next();
				result.addRow(row);
		
				if (min.hasNext())
					queue.add(min);
			}
		
//			log.debug(" merged " + tables.size() + " tables with " + result.rowCount() + " rows in " + (System.currentTimeMillis() - start) + " ms");
		}
		else if (tables.size() == 1)
			result = tables.get(0);
		
		if (timings != null)
			timings.end(Timings.TABLEMERGE);
		return result;
	}

	public static void verifySorted(GTable<String> table) {
		String x = null;
		int c = table.getColumn(table.getSortedColumn());
		for (String[] row : table.getRows()) {
			if (x != null) {
				if (x.compareTo(row[c]) > 0)
					log.error("sort verify failed");
			}
			x = row[c];
		}
		log.debug("verify done");
	}

	public static String getJoinAttribute(String[] row, int[] cols) {
		String s = "";
		for (int i : cols)
			s += row[i] + "_";
		return s;
	}
	
	public static String getJoinAttribute(Integer[] row, int[] cols) {
		String s = "";
		for (int i : cols)
			s += row[i] + "_";
		return s;
	}

	private static Integer[] combineRow(Integer[] lrow, Integer[] rrow, int rc) {
		Integer[] resultRow = new Integer[lrow.length + rrow.length - 1];
		System.arraycopy(lrow, 0, resultRow, 0, lrow.length);
		//		int s = 0, d = lrow.length;
		//		for (int i = 0; i < src.length; i++) {
		//			System.arraycopy(rrow, s, resultRow, d, src[i] - s);
		//			d += src[i] - s;
		//			s = src[i] + 1;
		//		}
		//		if (s < rrow.length)
		//			System.arraycopy(row, s, resultRow, d, resultRow.length - d);
		System.arraycopy(rrow, 0, resultRow, lrow.length, rc);
		System.arraycopy(rrow, rc + 1, resultRow, lrow.length + rc, rrow.length - rc - 1);
		return resultRow;
	}

	private static String[] combineRow(String[] lrow, String[] rrow, int rc) {
		String[] resultRow = new String[lrow.length + rrow.length - 1];
		System.arraycopy(lrow, 0, resultRow, 0, lrow.length);
		//		int s = 0, d = lrow.length;
		//		for (int i = 0; i < src.length; i++) {
		//			System.arraycopy(rrow, s, resultRow, d, src[i] - s);
		//			d += src[i] - s;
		//			s = src[i] + 1;
		//		}
		//		if (s < rrow.length)
		//			System.arraycopy(row, s, resultRow, d, resultRow.length - d);
		System.arraycopy(rrow, 0, resultRow, lrow.length, rc);
		System.arraycopy(rrow, rc + 1, resultRow, lrow.length + rc, rrow.length - rc - 1);
		return resultRow;
	}

	public static GTable<Integer> mergeJoinInteger(GTable<Integer> left, GTable<Integer> right, String col) {
		if (!left.isSorted() || !left.getSortedColumn().equals(col) || !right.isSorted() || !right.getSortedColumn().equals(col))
			throw new UnsupportedOperationException("merge join with unsorted tables");
		if (timings != null)
			timings.start(Timings.JOIN);
		long start = System.currentTimeMillis();
	
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!s.equals(col))
				resultColumns.add(s);
	
		int lc = left.getColumn(col);
		int rc = right.getColumn(col);
	
		GTable<Integer> result = new GTable<Integer>(resultColumns);
	
		int l = 0, r = 0;
		while (l < left.rowCount() && r < right.rowCount()) {
			Integer[] lrow = left.getRow(l);
			Integer[] rrow = right.getRow(r);
	
			if (lrow[lc].compareTo(rrow[rc]) < 0)
				l++;
			else if (lrow[lc].compareTo(rrow[rc]) > 0)
				r++;
			else {
				result.addRow(combineRow(lrow, rrow, rc));
	
				Integer[] row;
				int i = l + 1;
				while (i < left.rowCount() && left.getRow(i)[lc].compareTo(rrow[rc]) == 0) {
					row = left.getRow(i);
					result.addRow(combineRow(row, rrow, rc));
					i++;
				}
	
				int j = r + 1;
				while (j < right.rowCount() && lrow[lc].compareTo(right.getRow(j)[rc]) == 0) {
					row = right.getRow(j);
					result.addRow(combineRow(lrow, row, rc));
					j++;
				}
	
				l++;
				r++;
			}
		}
	
		result.setSortedColumn(lc);
	
//		log.debug(" joined (merge) " + left + " " + right + " => " + result + ", " + result.rowCount() + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		if (timings != null)
			timings.end(Timings.JOIN);
		return result;
	}

	public static GTable<String> mergeJoin(GTable<String> left, GTable<String> right, String col) {
		if (!left.isSorted() || !left.getSortedColumn().equals(col) || !right.isSorted() || !right.getSortedColumn().equals(col))
			throw new UnsupportedOperationException("merge join with unsorted tables");
		if (timings != null)
			timings.start(Timings.JOIN);
		long start = System.currentTimeMillis();
	
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!s.equals(col))
				resultColumns.add(s);
	
		int lc = left.getColumn(col);
		int rc = right.getColumn(col);
		
		log.debug("merge join: " + left + " x " + right);
	
		GTable<String> result = new GTable<String>(resultColumns);
	
		int l = 0, r = 0;
		while (l < left.rowCount() && r < right.rowCount()) {
			String[] lrow = left.getRow(l);
			String[] rrow = right.getRow(r);
	
			if (lrow[lc].compareTo(rrow[rc]) < 0)
				l++;
			else if (lrow[lc].compareTo(rrow[rc]) > 0)
				r++;
			else {
				result.addRow(combineRow(lrow, rrow, rc));
	
				String[] row;
				int i = l + 1;
				while (i < left.rowCount() && left.getRow(i)[lc].compareTo(rrow[rc]) == 0) {
					row = left.getRow(i);
					result.addRow(combineRow(row, rrow, rc));
					i++;
				}
	
				int j = r + 1;
				while (j < right.rowCount() && lrow[lc].compareTo(right.getRow(j)[rc]) == 0) {
					row = right.getRow(j);
					result.addRow(combineRow(lrow, row, rc));
					j++;
				}
	
				l++;
				r++;
			}
		}
	
		result.setSortedColumn(lc);
		log.debug("merge join: done in " + (System.currentTimeMillis() - start));
//		log.debug(" joined (merge) " + left + " " + right + " => " + result + ", " + result.rowCount() + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		if (timings != null)
			timings.end(Timings.JOIN);
		return result;
	}

	public static GTable<Integer> hashJoinInteger(GTable<Integer> left, GTable<Integer> right, List<String> cols) {
		if (timings != null)
			timings.start(Timings.JOIN);
//		long start = System.currentTimeMillis();
	
		if (left.rowCount() >= right.rowCount()) {
			GTable<Integer> tmpTable = left;
			left = right;
			right = tmpTable;
		}
	
		int[] lc = new int[cols.size()];
		for (int i = 0; i < lc.length; i++) {
			lc[i] = left.getColumn(cols.get(i));
		}
	
		int[] rc = new int[cols.size()];
		int[] src = new int[cols.size()];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = right.getColumn(cols.get(i));
			src[i] = right.getColumn(cols.get(i));
		}
	
		Arrays.sort(src);
	
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!cols.contains(s))
				resultColumns.add(s);
	
		GTable<Integer> result = new GTable<Integer>(resultColumns);
	
		Map<String,List<Integer[]>> leftVal2Rows = new HashMap<String,List<Integer[]>>();
		for (Integer[] row : left) {
			String joinAttribute = getJoinAttribute(row, lc);
			List<Integer[]> rows = leftVal2Rows.get(joinAttribute);
			if (rows == null) {
				rows = new ArrayList<Integer[]>();
				leftVal2Rows.put(joinAttribute, rows);
			}
			rows.add(row);
		}
	
		int count = 0;
		for (Integer[] row : right) {
			List<Integer[]> leftRows = leftVal2Rows.get(getJoinAttribute(row, rc));
			if (leftRows != null && leftRows.size() > 0) {
				for (Integer[] leftRow : leftRows) {
					Integer[] resultRow = new Integer[result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					int s = 0, d = leftRow.length;
					for (int i = 0; i < src.length; i++) {
						System.arraycopy(row, s, resultRow, d, src[i] - s);
						d += src[i] - s;
						s = src[i] + 1;
					}
					if (s < row.length)
						System.arraycopy(row, s, resultRow, d, resultRow.length - d);
					result.addRow(resultRow);
					count++;
				}
			}
		}
	
//		log.debug(" joined (hash) " + left + " " + right + " => " + result + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		if (timings != null)
			timings.end(Timings.JOIN);
	
		return result;
	}

	public static GTable<String> hashJoin(GTable<String> left, GTable<String> right, List<String> cols) {
		if (timings != null)
			timings.start(Timings.JOIN);
		long start = System.currentTimeMillis();
	
		if (left.rowCount() >= right.rowCount()) {
			GTable<String> tmpTable = left;
			left = right;
			right = tmpTable;
		}
	
		int[] lc = new int[cols.size()];
		for (int i = 0; i < lc.length; i++) {
			lc[i] = left.getColumn(cols.get(i));
		}
	
		int[] rc = new int[cols.size()];
		int[] src = new int[cols.size()];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = right.getColumn(cols.get(i));
			src[i] = right.getColumn(cols.get(i));
		}
	
		Arrays.sort(src);
	
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!cols.contains(s))
				resultColumns.add(s);
	
		GTable<String> result = new GTable<String>(resultColumns);
	
		Map<String,List<String[]>> leftVal2Rows = new HashMap<String,List<String[]>>();
		for (String[] row : left) {
			String joinAttribute = getJoinAttribute(row, lc);
			List<String[]> rows = leftVal2Rows.get(joinAttribute);
			if (rows == null) {
				rows = new ArrayList<String[]>();
				leftVal2Rows.put(joinAttribute, rows);
			}
			rows.add(row);
		}
	
		int count = 0;
		for (String[] row : right) {
			List<String[]> leftRows = leftVal2Rows.get(getJoinAttribute(row, rc));
			if (leftRows != null && leftRows.size() > 0) {
				for (String[] leftRow : leftRows) {
					String[] resultRow = new String[result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					int s = 0, d = leftRow.length;
					for (int i = 0; i < src.length; i++) {
						System.arraycopy(row, s, resultRow, d, src[i] - s);
						d += src[i] - s;
						s = src[i] + 1;
					}
					if (s < row.length)
						System.arraycopy(row, s, resultRow, d, resultRow.length - d);
					result.addRow(resultRow);
					count++;
				}
			}
		}
	
//		log.debug(" joined (hash) " + left + " " + right + " => " + result + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		if (timings != null)
			timings.end(Timings.JOIN);
	
		return result;
	}

	private static void sortTableComplete(GTable<String> table) {
		List<String> colsSorted = table.getColumnNamesSorted();
		final int[] cols = new int[table.columnCount()];
		for (int i = 0; i < table.columnCount(); i++)
			cols[i] = table.getColumn(colsSorted.get(i));
		Collections.sort(table.getRows(), new Comparator<String[]>() {
			public int compare(String[] r1, String[] r2) {
				for (int col : cols) {
					int x = r1[col].compareTo(r2[col]);
					if (x != 0)
						return x;
				}
				return 0;
			}
		});
	}

	private static void compareTables(GTable<String> t1, GTable<String> t2) {
		sortTableComplete(t1);
		sortTableComplete(t2);
	
		List<String> colsSorted = t1.getColumnNamesSorted();
	
		boolean same = true;
		for (int i = 0; i < Math.min(t1.rowCount(), t2.rowCount()); i++) {
			String[] r1 = t1.getRow(i);
			String[] r2 = t2.getRow(i);
	
			for (String col : colsSorted)
				if (!r1[t1.getColumn(col)].equals(r2[t2.getColumn(col)])) {
					same = false;
					break;
				}
	
			if (!same) {
				log.debug("not same");
				break;
			}
		}
		log.debug(t1 + " == " + t2 + ": " + same);
	}
}
