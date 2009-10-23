package edu.unika.aifb.graphindex.algorithm.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.NodeElement;
import edu.unika.aifb.graphindex.util.Statistics;

public class GraphIsomorphism {
	
	private static final Logger log = Logger.getLogger(GraphIsomorphism.class);
	
	private Map<String,Table<String>> getEdgeTables(DirectedMultigraph<NodeElement,EdgeElement> g) {
		HashMap<String,Table<String>> tables = new HashMap<String,Table<String>>();
		for (EdgeElement edge : g.edgeSet()) {
			Table<String> t = tables.get(edge.getLabel());
			if (t == null) {
				t = new Table<String>("s", "o");
				tables.put(edge.getLabel(), t);
			}
			t.addRow(new String[] { edge.getSource().getLabel(), edge.getTarget().getLabel() });
		}
		return tables;
	}
	
	public List<Map<String,String>> getIsomorphicMappings(DirectedMultigraph<NodeElement,EdgeElement> g1, DirectedMultigraph<NodeElement,EdgeElement> g2) {
		List<Map<String,String>> maps = new ArrayList<Map<String,String>>();

		Statistics.inc(this, Statistics.Counter.ISO_ALL);
		
		if (g1.edgeSet().size() != g2.edgeSet().size() || g1.vertexSet().size() != g2.vertexSet().size())
			return maps;
		
		
//		Map<String,Table<String>> g1tables = getEdgeTables(g1);
		Set<String> g1labels = new HashSet<String>(10);
		for (EdgeElement edge : g1.edgeSet())
			g1labels.add(edge.getLabel());
		
		Map<String,Table<String>> g2tables = getEdgeTables(g2);

		if (!g1labels.equals(g2tables.keySet()))
			return maps;

		Statistics.inc(this, Statistics.Counter.ISO_CHECK);
		
		List<Table<String>> resultTables = new ArrayList<Table<String>>();
		Queue<EdgeElement> toVisit = new LinkedList<EdgeElement>(g1.edgeSet()); 
		
		while (toVisit.size() > 0) {
			EdgeElement currentEdge = toVisit.poll();
			String property = currentEdge.getLabel();
			String source = currentEdge.getSource().getLabel();
			String target = currentEdge.getTarget().getLabel();
			
			Table<String> sourceTable = null, targetTable = null;
			for (Table<String> table : resultTables) {
				if (table.hasColumn(source))
					sourceTable = table;
				if (table.hasColumn(target))
					targetTable = table;
			}
			
			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			Table<String> currentTable = new Table<String>(g2tables.get(property), true);
			currentTable.setColumnName(0, source);
			currentTable.setColumnName(1, target);

			Table<String> result = null;
			if (sourceTable == null && targetTable == null) {
				result = currentTable;
			}
			else if (sourceTable == null && targetTable != null) {
				currentTable.sort(1);
				targetTable.sort(target);
				
				result = Tables.mergeJoin(currentTable, targetTable, target);
			}
			else if (sourceTable != null && targetTable == null) {
				currentTable.sort(0);
				sourceTable.sort(source);
				
				result = Tables.mergeJoin(currentTable, sourceTable, source);
			}
			else {
				if (sourceTable == targetTable) {
					currentTable.sort(Arrays.asList(source, target));
					sourceTable.sort(Arrays.asList(source, target));
					result = Tables.mergeJoin(currentTable, sourceTable, Arrays.asList(source, target));
				}
				else {
					currentTable.sort(0);
					sourceTable.sort(source);
					
					result = Tables.mergeJoin(currentTable, sourceTable, source);
					
					result.sort(target);
					targetTable.sort(target);
					
					result = Tables.mergeJoin(result, targetTable, target);
				}
			}
			
			if (result == null || result.rowCount() == 0) {
				return maps;
			}
			
			resultTables.add(result);
		}
		
		if (resultTables.size() == 0 || resultTables.get(0).rowCount() == 0)
			return maps;

		Table<String> result = resultTables.get(0);
		for (String[] row : result) {
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < row.length; i++)
				map.put(result.getColumnName(i), row[i]);
			maps.add(map);
		}
		
		Statistics.inc(this, Statistics.Counter.ISO_END);
		
		return maps;
	}
}
