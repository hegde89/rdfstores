package edu.unika.aifb.graphindex.vp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public class VPQueryEvaluator {
	private LuceneStorage m_ls;
	
	private final static Logger log = Logger.getLogger(VPQueryEvaluator.class);
	
	public VPQueryEvaluator(LuceneStorage ls) {
		m_ls = ls;
	}

	private String getJoinAttribute(String[] row, int[] cols) {
		String s = "";
		for (int i : cols)
			s += row[i] + "_";
		return s;
	}

	private GTable<String> join(GTable<String> left, GTable<String> right, List<String> cols) {
		long start = System.currentTimeMillis();
		
		if (left.rowCount() >= right.rowCount()) {
//			log.debug("should swap");

			GTable<String> tmpTable = left;
			left = right;
			right = tmpTable;
		}
		
//		log.debug(left + " " + right + " " + cols);
		
		int[] lc = new int [cols.size()];
		for (int i = 0; i < lc.length; i++) {
			lc[i] = left.getColumn(cols.get(i));
		}
		
		int[] rc = new int [cols.size()];
		int[] src = new int [cols.size()];
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
					String[] resultRow = new String [result.columnCount()];
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
//					if (count % 100000 == 0)
//						log.debug(" rows: " + count);
				}
			}
		}
		log.debug(left + " " + right + " => " + result + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		return result;
	}
	
	private GTable<String> getTable(String subject, String property, String object) {
		if (subject.startsWith("?"))
			subject = null;
		
		if (object.startsWith("?"))
			object = null;
		
		GTable<String> table = m_ls.getTable(subject, property, object);
		
		return table;
	}

	public void evaluate(Query q) throws StorageException {
		long start = System.currentTimeMillis();
		Graph<QueryNode> queryGraph = q.toGraph();
		
		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {

			public int compare(GraphEdge<QueryNode> o1, GraphEdge<QueryNode> o2) {
				// TODO Auto-generated method stub
				return 0;
			}
			
		});
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();
		
		toVisit.addAll(queryGraph.edges());
		
		List<GTable<String>> results = new ArrayList<GTable<String>>();

		boolean empty = false;;
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			if (visited.contains(currentEdge))
				continue;
			
			visited.add(currentEdge);
			
			String srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String dstLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			log.debug(srcLabel + " -> " + dstLabel);
			
			GTable<String> table = getTable(srcLabel, currentEdge.getLabel(), dstLabel);
			table.setColumnName(0, srcLabel);
			table.setColumnName(1, dstLabel);
			
			GTable<String> left = null, right = null;
			for (GTable<String> t : results) {
				if (t.hasColumn(srcLabel))
					left = t;
				if (t.hasColumn(dstLabel))
					right = t;
			}

			GTable<String> result;
			
			if (left == null && right == null) {
				result = table;
			}
			else if (left == null) {
				result = join(table, right, Arrays.asList(dstLabel));
			}
			else if (right == null) {
				result = join(left, table, Arrays.asList(srcLabel));
			}
			else {
				// edge is between two intermediary results
				// we need to load triples from the dst ext with the label of the current edge
				// probably use the objects already mapped there

				Set<String> objects = new HashSet<String>();
				int col = right.getColumn(dstLabel);
				for (String[] t : right) {
					objects.add(t[col]);
				}
				
				GTable<String> middle = new GTable<String>(Arrays.asList(srcLabel, dstLabel));
				
				for (String[] row : table) {
					if (objects.contains(row[1]))
						middle.addRow(row);
				}
				
				
				if (left.rowCount() < right.rowCount()) {
					result = join(left, middle, Arrays.asList(srcLabel));
					result = join(result, right, Arrays.asList(dstLabel));
				}
				else {
					result = join(middle, right, Arrays.asList(dstLabel));
					result = join(left, result, Arrays.asList(srcLabel));
				}
			}
			
			results.remove(left);
			results.remove(right);
			
			if (result.rowCount() > 0)
				results.add(result);
			else {
				empty  = true;
				break;
			}
		}
		
		if (empty) {
			log.debug("size: 0");
		}
		else {
			log.debug("size: " + results.get(0).rowCount());
		}
		log.debug("duration: " + (System.currentTimeMillis() - start) / 1000.0);
	}
}
