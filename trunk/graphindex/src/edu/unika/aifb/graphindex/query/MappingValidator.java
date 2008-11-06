package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class MappingValidator implements Callable<Set<Map<String,String>>> {

	private Graph<QueryNode> m_queryGraph;
	private VertexMapping m_mapping;
	private ExtensionManager m_em;
	private StatisticsCollector m_collector;
	private Timings t;
	private ExtensionStorage m_es;
	private static final Logger log = Logger.getLogger(MappingValidator.class);
	
	public MappingValidator(StructureIndex index, Graph<QueryNode> queryGraph, VertexMapping iso, StatisticsCollector collector) {
		m_mapping = iso;
		m_queryGraph = queryGraph;
		m_em = index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
		m_collector = collector;
		t = new Timings();
	}
	
	private String getJoinAttribute(String[] row, int[] cols) {
		String s = "";
		for (int i : cols)
			s += row[i] + "_";
		return s;
	}
	
	private GTable<String> join(GTable<String> left, GTable<String> right, List<String> cols) {
		t.start(Timings.JOIN);
		
		if (left.rowCount() >= right.rowCount()) {
			log.debug("should swap");

//			List<String> tmp = leftCols;
//			leftCols = rightCols;
//			rightCols = tmp;
//			
//			Table tmpTable = left;
//			left = right;
//			right = tmpTable;
		}
		
		log.debug(left + " " + right + " " + cols);
		
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
						s = src[i] + 1;
						d += src[i] - s + 1;
					}
					System.arraycopy(row, s, resultRow, d, resultRow.length - d);
//					System.arraycopy(row, 0, resultRow, leftRow.length, rc);
//					System.arraycopy(row, rc + 1, resultRow, leftRow.length + rc, row.length - rc - 1);
//					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
//					System.arraycopy(row, 0, resultRow, leftRow.length, row.length);
					result.addRow(resultRow);
					count++;
					if (count % 100000 == 0)
						log.debug(" rows:" + count);
				}
			}
		}
		t.end(Timings.JOIN);
		return result;
	}
	
	private GTable<String> toTable(String leftCol, String rightCol, List<Triple> triples) {
		GTable<String> table = new GTable<String>(leftCol, rightCol);
		for (Triple t : triples)
			table.addRow(new String [] { t.getSubject(), t.getObject() });
		return table;
	}
	
	private Set<Map<String,String>> validateMapping(Graph<QueryNode> queryGraph, VertexMapping vm) throws StorageException {
		GTable<String> result = null;
		
		Stack<Integer> toVisit = new Stack<Integer>();
		Set<Integer> visited = new HashSet<Integer>();
		Set<GraphEdge<QueryNode>> usedEdges = new HashSet<GraphEdge<QueryNode>>();
		
		for (int node = 0; node < queryGraph.nodeCount(); node++) {
			if (queryGraph.inDegreeOf(node) == 0) {
				if (toVisit.size() == 0)
					toVisit.push(node);
			}
		}
		
		while (toVisit.size() > 0) {
			int currentNode = toVisit.pop();
			
			if (visited.contains(currentNode))
				continue;
			
			visited.add(currentNode);
			
			List<GraphEdge<QueryNode>> edges = new ArrayList<GraphEdge<QueryNode>>();
			
			for (GraphEdge<QueryNode> e : queryGraph.incomingEdges(currentNode)) {
				toVisit.push(e.getSrc());
			}
			
			for (GraphEdge<QueryNode> e : queryGraph.outgoingEdges(currentNode)) {
				toVisit.push(e.getDst());
				if (!usedEdges.contains(e))
					edges.add(e);
			}
			
			String nodeLabel = queryGraph.getNode(currentNode).getSingleMember();
			String nodeExt = vm.getVertexCorrespondence(nodeLabel, true);
			
			for (GraphEdge<QueryNode> edge : edges) {
				String otherLabel = queryGraph.getNode(edge.getDst()).getSingleMember();
				String otherExt = vm.getVertexCorrespondence(otherLabel, true);
				
				GTable<String> otherTable;
				if (otherLabel.startsWith("?"))
					otherTable = m_es.getTable(otherExt, edge.getLabel(), null);
				else
					otherTable = m_es.getTable(otherExt, edge.getLabel(), otherLabel);
				
				otherTable.setColumnName(0, nodeLabel);
				otherTable.setColumnName(1, otherLabel);
				
				if (result == null) {
					if (queryGraph.inDegreeOf(currentNode) > 0) {
						List<Triple> nodeTriples = new ArrayList<Triple>();
						GraphEdge<QueryNode> ine = queryGraph.incomingEdges(currentNode).get(0);
						if (nodeLabel.startsWith("?"))
							nodeTriples.addAll(m_es.getTriples(nodeExt, ine.getLabel()));
						else
							nodeTriples.addAll(m_es.getTriples(nodeExt, ine.getLabel(), nodeLabel));
						
						GTable<String> nodeTable = toTable(queryGraph.getNode(ine.getSrc()).getSingleMember(), nodeLabel, nodeTriples);
						
						result = join(nodeTable, otherTable, Arrays.asList(nodeLabel));
					}
					else {
						result = new GTable<String>(Arrays.asList(nodeLabel, otherLabel));
						for (String[] t : otherTable)
							result.addRow(new String [] { t[0], t[1] });
					}
				}
				else {
					if (result.hasColumn(otherLabel))
						result = join(result, otherTable, Arrays.asList(nodeLabel, otherLabel));
					else
						result = join(result, otherTable, Arrays.asList(nodeLabel));
				}
				usedEdges.add(edge);

				if (result.rowCount() == 0)
					return new HashSet<Map<String,String>>();
			}
		}
//		log.debug(vm);
//		log.debug(result.toDataString());

		Set<Map<String,String>> resultMappings = new HashSet<Map<String,String>>();
		for (String[] row : result) {
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < row.length; i++) {
				map.put(result.getColumnNames()[i], row[i]);
			}
			resultMappings.add(map);
 		}
		
		return resultMappings;
	}
	
	public Set<Map<String,String>> call() throws Exception {
		Set<Map<String,String>> set = validateMapping(m_queryGraph, m_mapping);
		m_collector.addTimings(t);
		return set;
	}

}
