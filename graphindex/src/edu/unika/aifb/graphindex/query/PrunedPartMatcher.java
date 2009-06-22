package edu.unika.aifb.graphindex.query;

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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

public class PrunedPartMatcher extends AbstractIndexGraphMatcher {

	private List<GraphEdge<QueryNode>> m_part;
	private String m_startNode;
	private GTable<String> m_startNodeTable;
	private Set<String> m_validExtensions;
	
	private static final Logger log = Logger.getLogger(PrunedPartMatcher.class);

	protected PrunedPartMatcher(StructureIndex index, String graphName) {
		super(index, graphName);

	}

	@Override
	protected boolean isCompatibleWithIndex() {
		return true;
	}

	public void setPrunedPart(List<GraphEdge<QueryNode>> part, String startNode, GTable<String> startNodeTable) {
		m_part = part;
		m_startNode = startNode;
		m_startNodeTable = startNodeTable;
		m_validExtensions = new HashSet<String>(200);
	}
	
	public Set<String> getValidExtensions() {
		return m_validExtensions;
	}
	
	public void match() throws StorageException {
		final Graph<QueryNode> queryGraph = m_qe.getQueryGraph();
		final Map<String,Integer> proximities = m_qe.getProximities();
		
		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
								
				int e1score = proximities.get(s1) * proximities.get(d1);
				int e2score = proximities.get(s2) * proximities.get(d2);
				
				if (e1score == e2score) {
					Integer ce1 = m_qe.getIndex().getObjectCardinality(e1.getLabel());
					Integer ce2 = m_qe.getIndex().getObjectCardinality(e2.getLabel());
					
					if (ce1 != null && ce2 != null && ce1.intValue() != ce2.intValue()) {
						if (ce1 < ce2)
							return 1;
						else
							return -1;
					}
				}
				
				if (e1score < e2score)
					return -1;
				else
					return 1;
			}
		});
		
		toVisit.addAll(m_part);
		Set<String> visited = new HashSet<String>();
		visited.add(m_startNode);
		
		List<GTable<String>> resultTables = new ArrayList<GTable<String>>();
		resultTables.add(m_startNodeTable);
		
		while (toVisit.size() > 0) {
			String property, srcLabel, trgLabel;
			GraphEdge<QueryNode> currentEdge;
			
			List<GraphEdge<QueryNode>> skipped = new ArrayList<GraphEdge<QueryNode>>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
				trgLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			}
			while ((!visited.contains(srcLabel) && !visited.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel)));

			visited.add(srcLabel);
			visited.add(trgLabel);
			
			log.debug(" " + srcLabel + " -> " + trgLabel + " (" + property + ")");
			
			GTable<String> sourceTable = null, targetTable = null, result;
			for (GTable<String> table : resultTables) {
				if (table.hasColumn(srcLabel))
					sourceTable = table;
				if (table.hasColumn(trgLabel))
					targetTable = table;
			}

			resultTables.remove(sourceTable);
			resultTables.remove(targetTable);

			log.debug(" src table: " + sourceTable + ", trg table: " + targetTable);
			
			if (sourceTable == null && targetTable != null) {
				// cases 1 a,d: edge has one unprocessed node, the source
				GTable<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);

				targetTable.sort(trgLabel, true);
				result = Tables.mergeJoin(targetTable, edgeTable, trgLabel);
			} else if (sourceTable != null && targetTable == null) {
				// cases 1 b,c: edge has one unprocessed node, the target
				GTable<String> edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);

				sourceTable.sort(srcLabel, true);
				result = Tables.mergeJoin(sourceTable, edgeTable, srcLabel);
			} else if (sourceTable != null && targetTable != null){
				// case 3: both nodes already processed
				if (sourceTable == targetTable) {
					result = Tables.mergeJoin(sourceTable, getEdgeTable(property, srcLabel, trgLabel, 0), Arrays.asList(srcLabel, trgLabel));
				}
				else {
					GTable<String> first, second;
					String firstCol, secondCol;
					GTable<String> edgeTable;
	
					// start with smaller table
					if (sourceTable.rowCount() < targetTable.rowCount()) {
						first = sourceTable;
						firstCol = srcLabel;
	
						second = targetTable;
						secondCol = trgLabel;
	
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 0);
					} else {
						first = targetTable;
						firstCol = trgLabel;
	
						second = sourceTable;
						secondCol = srcLabel;
	
						edgeTable = getEdgeTable(property, srcLabel, trgLabel, 1);
					}
	
					first.sort(firstCol, true);
					result = Tables.mergeJoin(first, edgeTable, firstCol);
	
					result.sort(secondCol, true);
					second.sort(secondCol, true);
					result = Tables.mergeJoin(second, result, secondCol);
				}
			}
			else {
				log.error("what");
				result = null;
			}
		
			if (result == null || result.rowCount() == 0) {
				resultTables.clear();
				break;
			}
			
			resultTables.add(result);
		}

		for (GTable<String> resultTable : resultTables) {
			if (resultTable.hasColumn(m_startNode)) {
				int col = resultTable.getColumn(m_startNode);
				for (String[] row : resultTable)
					m_validExtensions.add(row[col]);
				break;
			}
		}
	}
	
	private GTable<String> getEdgeTable(String property, String srcLabel, String trgLabel, int orderedBy) throws StorageException {
		GTable<String> edgeTable;
		if (orderedBy == 0)
			edgeTable = m_p2ts.get(property);
		else
			edgeTable = m_p2to.get(property);

		if (edgeTable == null) {
			throw new UnsupportedOperationException("error");
		}

		GTable<String> table = new GTable<String>(srcLabel, trgLabel);
		table.setRows(edgeTable.getRows());
		table.setSortedColumn(orderedBy);

		log.debug(" edge table rows: " + table.rowCount());
		return table;
	}

}
