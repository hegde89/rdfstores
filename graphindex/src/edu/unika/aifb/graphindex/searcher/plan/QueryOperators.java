package edu.unika.aifb.graphindex.searcher.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQueryPart;
import edu.unika.aifb.graphindex.searcher.structured.PrunedPartMatcher;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryOperators {
	private IndexReader m_idxReader;
	private Map<IndexDescription,IndexStorage> m_indexes;
	private PrunedPartMatcher m_matcher;

	private static final Logger log = Logger.getLogger(QueryOperators.class);
	
	public QueryOperators(IndexReader idxReader, Map<IndexDescription,IndexStorage> indexes) throws IOException, StorageException {
		m_idxReader = idxReader;
		m_indexes = indexes;

		m_matcher = new PrunedPartMatcher(idxReader);
		m_matcher.initialize();
	}
	
	public Table<String> load(IndexDescription index, DataField[] columns, String[] columnNames, Object... indexValues) throws StorageException {
		long start = System.currentTimeMillis();
		Table<String> t = m_indexes.get(index).getTable(index, columns, index.createValueArray(indexValues));
		for (int i = 0; i < columnNames.length; i++)
			t.setColumnName(i, columnNames[i]);
		log.debug("load: " + t + " (" + (System.currentTimeMillis() - start) + ")");
		return t;
	}
	
	private void sort(Table<String> t, List<String> columns) {
		if (columns.size() > 1)
			t.sort(columns);
		else
			t.sort(columns.get(0), true);
	}
	
	
	public Table<String> mergeJoin(Table<String> t1, Table<String> t2, String... columns) {
		Table<String> t = mergeJoin(t1, t2, Arrays.asList(columns));
		return t;
	}
	
	public Table<String> mergeJoin(Table<String> t1, Table<String> t2, List<String> columns) {
		long start = System.currentTimeMillis();
		sort(t1, columns);
		sort(t2, columns);
		
		Table<String> t = Tables.mergeJoin(t1, t2, columns);
		
		log.debug("merge join: " + t1 + " " + t2 + " => " + t + " (" + (System.currentTimeMillis() - start) + ")");
		return t;
	}
	
	public Table<String> indexJoin(Table<String> t1, String column, IndexDescription index, DataField[] columns, String[] columnNames, DataField joinField, Object... indexValues) throws StorageException {
		long start = System.currentTimeMillis();
		Table<String> t2 = new Table<String>(columns);
		for (int i = 0; i < columnNames.length; i++)
			t2.setColumnName(i, columnNames[i]);

		Object[] indexValuesStub = new Object[indexValues.length + 2];
		for (int i = 0; i < indexValues.length; i++)
			indexValuesStub[i] = indexValues[i];
 		indexValuesStub[indexValues.length] = joinField;
		
		int col = t1.getColumn(column);
		Set<String> values = new HashSet<String>();
		for (String[] row : t1) {
			if (values.add(row[col])) {
				indexValuesStub[indexValues.length + 1] = row[col];
				t2.addRows(m_indexes.get(index).getTable(index, columns, index.createValueArray(indexValuesStub)).getRows());
			}
		}
		
		t1.sort(column, true);
		t2.sort(column, true);
		Table<String> t = Tables.mergeJoin(t1, t2, column);
		
		log.debug("index join: " + t1 + " " + t2 + " => " + t + " (" + (System.currentTimeMillis() - start) + ")");
		
		return t;
	}
	
	public Table<String> indexJoin(Table<String> t1, String indexColumn, String joinColumn, IndexDescription index, DataField[] columns, String[] columnNames, DataField joinField, Object... indexValues) throws StorageException {
		long start = System.currentTimeMillis();
		Table<String> t2 = new Table<String>(columns);
		for (int i = 0; i < columnNames.length; i++)
			t2.setColumnName(i, columnNames[i]);

		Object[] indexValuesStub = new Object[indexValues.length + 2];
		for (int i = 0; i < indexValues.length; i++)
			indexValuesStub[i] = indexValues[i];
 		indexValuesStub[indexValues.length] = joinField;
		
		int col = t1.getColumn(indexColumn);
		Set<String> values = new HashSet<String>();
		for (String[] row : t1) {
			if (values.add(row[col])) {
				indexValuesStub[indexValues.length + 1] = row[col];
				t2.addRows(m_indexes.get(index).getTable(index, columns, index.createValueArray(indexValuesStub)).getRows());
			}
		}
		
		List<String> joinColumns = Arrays.asList(indexColumn, joinColumn);
		
		sort(t1, joinColumns);
		sort(t2, joinColumns);
		Table<String> t = Tables.mergeJoin(t1, t2, joinColumns);
		
		log.debug("index join: " + t1 + " " + t2 + " => " + t  + " (" + (System.currentTimeMillis() - start) + ")");
		
		return t;
	}
	
	public Table<String> refineWithPrunedPart(PrunedQueryPart prunedQueryPart, String label, Table<String> result) throws StorageException {
		long start = System.currentTimeMillis();
		Map<String,List<String[]>> ext2entity = new HashMap<String,List<String[]>>(100);
		Table<String> extTable = new Table<String>(Arrays.asList(label));
		
		int col = result.getColumn(label);
		
		Set<String> values = new HashSet<String>();
		for (String[] row : result) {
			String ext = m_indexes.get(IndexDescription.SES).getDataItem(IndexDescription.SES, DataField.EXT_SUBJECT, row[col]);

			if (values.add(ext))
				extTable.addRow(new String[] { ext });
			
			List<String[]> entities = ext2entity.get(ext);
			if (entities == null) {
				entities = new ArrayList<String[]>(200);
				ext2entity.put(ext, entities);
			}
			entities.add(row);
		}
		
		m_matcher.setPrunedPart(prunedQueryPart, label, extTable);
		m_matcher.match();
		
		int rows = result.rowCount();
		result = new Table<String>(result, false);
		for (String ext : m_matcher.getValidExtensions()) {
			result.addRows(ext2entity.get(ext));
		}
		log.debug("refined: " + rows + " => " + result.rowCount() + " (" + (System.currentTimeMillis() - start) + ")");
		
		return result;
	}
	

	public Table<String> compact(Table<String> table, List<String> columns) {
		Table<String> result = new Table<String>(columns);
		
		int[] cols = new int [columns.size()];
		for (int i = 0; i < columns.size(); i++)
			cols[i] = table.getColumn(columns.get(i));
		
		Set<String> sigs = new HashSet<String>();
		for (String[] row : table) {
			String[] selectRow = new String [cols.length];
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < cols.length; i++) {
				selectRow[i] = row[cols[i]];
				sb.append(row[cols[i]]).append("__");
			}
			
			String sig = sb.toString();
			if (sigs.add(sig))
				result.addRow(selectRow);
		}
		
		log.debug("compact: " + table.rowCount() + " => " + result.rowCount());
		
		return result;
	}
}
