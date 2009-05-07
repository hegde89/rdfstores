package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class JoinMatcher {

	private Graph<QueryNode> m_queryGraph;
	private HashMap<String,GTable<String>> m_p2ts;
	private HashMap<String,GTable<String>> m_p2to;
	private Timings m_timings;
	private ExtensionStorage m_es;
	private GraphStorage m_gs;
	private Set<String> m_signatureNodes;
	private Set<String> m_joinNodes;
	private Map<String,Integer> m_joinCounts;
	
	private final static Logger log = Logger.getLogger(JoinMatcher.class);
	private boolean m_purgeNeeded;
	private HashMap<String,Integer> m_inDegree;
	private StructureIndex m_index;
	
	public JoinMatcher(StructureIndex index, String graphName) throws StorageException {
		m_index = index;
		m_es = index.getExtensionManager().getExtensionStorage();
		m_gs = index.getGraphManager().getGraphStorage();
		
		m_p2ts = new HashMap<String,GTable<String>>();
		m_p2to = new HashMap<String,GTable<String>>();
		
		m_inDegree = new HashMap<String,Integer>();
		
		Set<LabeledEdge<String>> edges = m_gs.loadEdges(graphName);
		
		long start = System.currentTimeMillis();
		for (LabeledEdge<String> e : edges) {

			GTable<String> table = m_p2ts.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2ts.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });

			table = m_p2to.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2to.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });
			
			if (!m_inDegree.containsKey(e.getSrc()))
				m_inDegree.put(e.getSrc(), 0);
			
			Integer deg = m_inDegree.get(e.getDst());
			if (deg == null)
				m_inDegree.put(e.getDst(), 1);
			else
				m_inDegree.put(e.getDst(), deg + 1);
		}
		
		for (GTable<String> t : m_p2ts.values())
			t.sort(0);
		for (GTable<String> t : m_p2to.values())
			t.sort(1);
		
		log.debug(System.currentTimeMillis() - start);
	}
	
	private GTable<String> purgeTable(GTable<String> table, Integer newColumn) {
		long start = System.currentTimeMillis();
		GTable<String> purged = new GTable<String>(table, false);
		
		List<Integer> sigCols = getSignatureColumns(table);
//		log.debug("sig cols: " + sigCols + " " + sigCols.size());
//		log.debug("new col: " + newColumn);

		if (sigCols.size() == 0 || sigCols.size() == table.columnCount())
			return table;
		
		boolean nopurge = false;
		if (!m_purgeNeeded && newColumn != null && sigCols.contains(newColumn.intValue())) {
			log.debug("new column is a sig col, no purge needed");
			nopurge = true;
			purged.setRows(table.getRows());
			return purged;
		}
		
		Set<String> signatures = new HashSet<String>(table.rowCount() / 2);
		for (String[] row : table) {
			String sig = getSignature(row, sigCols);
			if (signatures.add(sig)) 
				purged.addRow(row);
		}
		log.debug("purged " + table.rowCount() + " => " + purged.rowCount() + " in " + (System.currentTimeMillis() - start) + " msec");
		if (table.rowCount() > purged.rowCount()) {
//			log.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			if (nopurge)
				log.debug("??????");
		}
		return purged;
	}
	
//	private GTable<String> purgeTable(GTable<String> table, boolean includeJoinNodes) {
//		if (includeJoinNodes)
//			return purgeTable(table, m_joinNodes.toArray(new String[]{}));
//		else 
//			return purgeTable(table);
//	}
	
	public void setQueryGraph(Query query, Graph<QueryNode> queryGraph) {
		m_queryGraph = queryGraph;
		m_signatureNodes = new HashSet<String>();
		m_joinNodes = new HashSet<String>();
		m_joinCounts = new HashMap<String,Integer>();
		
		for (int i = 0; i < m_queryGraph.nodeCount(); i++) {
			if (m_queryGraph.inDegreeOf(i) > 0)
				m_signatureNodes.add(m_queryGraph.getNode(i).getSingleMember());
			else if (m_queryGraph.outDegreeOf(i) > 1)
				m_joinNodes.add(m_queryGraph.getNode(i).getSingleMember());
			
			m_joinCounts.put(m_queryGraph.getNode(i).getSingleMember(), m_queryGraph.inDegreeOf(i) + m_queryGraph.outDegreeOf(i));
		}
		
		if (query.getRemovedNodes().size() > 0) {
			for (String node : query.getRemovedNodes()) {
				if (!m_signatureNodes.contains(node))
					m_signatureNodes.remove(node);
			}
		}
		
		log.debug("sig nodes: " + m_signatureNodes);
		log.debug("join nodes: " + m_joinNodes);
//		log.debug("join counts: " + m_joinCounts);
	}
	
	public void setTimings(Timings timings) {
		m_timings = timings;
	}
	
	private String getSourceColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_src";
		return m_queryGraph.getNode(edge.getSrc()).getName();
	}
	
	private String getTargetColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_dst";
		return m_queryGraph.getNode(edge.getDst()).getName();
	}
	
	private List<Integer> getSignatureColumns(GTable<String> table) {
		List<Integer> sigCols = new ArrayList<Integer>();

		for (String colName : table.getColumnNames())
			if (m_signatureNodes.contains(colName) || (m_joinNodes.contains(colName) && m_joinCounts.get(colName) > 0)) 
				sigCols.add(table.getColumn(colName));

		return sigCols;
	}
	
	private String getSignature(String[] row, List<Integer> sigCols) {
		StringBuilder sb = new StringBuilder();
		for (int sigCol : sigCols)
			sb.append(row[sigCol]).append("__");
		return sb.toString();
	}
	
	private GTable<String> getTable(GraphEdge<QueryNode> queryEdge, int col, GTable<String> tripleExts) throws StorageException {
		QueryNode qnSrc = m_queryGraph.getNode(queryEdge.getSrc());
		QueryNode qnDst = m_queryGraph.getNode(queryEdge.getDst());
		String srcNode = qnSrc.getName();
		String dstNode = qnDst.getName();

		GTable<String> edgeTable;
		if (col == 0)
			edgeTable = m_p2ts.get(queryEdge.getLabel());
		else
			edgeTable = m_p2to.get(queryEdge.getLabel());
		
		if (qnSrc.hasVariables() && qnDst.hasVariables()) {
			GTable<String> table = new GTable<String>(edgeTable);
			table.setColumnName(0, srcNode);
			table.setColumnName(1, dstNode);
			return purgeTable(table, null);
		}
		
		boolean checkSrc = qnSrc.hasGroundTerms() && !qnSrc.hasVariables();
		boolean checkDst = qnDst.hasGroundTerms() && !qnDst.hasVariables();
		
		String validObjectExt = null;
		Set<String> validSubjectExts = new HashSet<String>();

		m_timings.start(Timings.GT);
		boolean checkSrcFromDst = false;
		if (checkSrc)
			validSubjectExts = new HashSet<String>(m_es.getExtensions(Index.SE, qnSrc.getSingleMember()));
		if (checkDst) {
			validObjectExt = m_es.getExtension(qnDst.getSingleMember());
			
			if (m_queryGraph.inDegreeOf(queryEdge.getSrc()) > 0) {
				GTable<String> t = m_es.getIndexTable(Index.EPO, validObjectExt, queryEdge.getLabel(), qnDst.getSingleMember());
				if (tripleExts != null) {
					tripleExts.setColumnName(0, srcNode);
					tripleExts.setColumnName(1, dstNode);
				}

				for (String[] row : t) {
					String subjectExt = m_es.getExtension(row[0]);
					if (!subjectExt.equals("")) {
						validSubjectExts.add(subjectExt);
						tripleExts.addRow(row);
					}
				}
				log.debug("valid subject exts: " + validSubjectExts.size());
				checkSrcFromDst = true;
			}
			else {
				tripleExts.setRows(m_es.getIndexTable(Index.EPO, validObjectExt, queryEdge.getLabel(), qnDst.getSingleMember()).getRows());
				tripleExts.setSortedColumn(0);
			}
		}
		m_timings.end(Timings.GT);

		
		GTable<String> table = new GTable<String>(srcNode, dstNode);
		List<Integer> sigCols = getSignatureColumns(table);
		Set<String> signatures = new HashSet<String>(edgeTable.rowCount() / 2);
		log.debug(sigCols);
		log.debug(edgeTable);
		for (String[] row : edgeTable) {
			boolean foundAll = true;
			if (checkSrc && !validSubjectExts.contains(row[1]))
				foundAll = false;
			
			if (foundAll && checkSrcFromDst && !validSubjectExts.contains(row[0]))
				foundAll = false;
			
			if (foundAll && checkDst && !validObjectExt.equals(row[1]))
				foundAll = false;

			if (foundAll) {
				if (sigCols.size() == 1) {
					String sig = getSignature(row, sigCols);
					if (!signatures.contains(sig)) {
						table.addRow(row);
						signatures.add(sig);
					}
				}
				else
					table.addRow(row);
			}
		}
//		log.debug("=> " + table);
		table.setSortedColumn(col);

		return table;
	}
	
	private void filterTable(GTable<String> table, String property, String col, GTable<String> extTable, Map<String,GTable<String>> extTriples) throws StorageException {
		HashMap<String,GTable<String>> newExtTriples = new HashMap<String,GTable<String>>();
		if (table.getColumn(col) == 0) {
			// column is subject of table to filter
		}
		else if (table.getColumn(col) == 1) {
			// column is object of table to filter
			
			for (String[] extRow : extTable) {
				String ext = extRow[extTable.getColumn(col)];
				GTable<String> tripleTable = extTriples.get(ext);
				for (String[] tripleRow : tripleTable) {
					GTable<String> t = m_es.getIndexTable(Index.EPO, ext, property, tripleRow[tripleTable.getColumn(col)]);
					
				}
			}
		}
	}
	
	private void processed(String n1, String n2) {
		m_purgeNeeded = false;
		
		int c = m_joinCounts.get(n1) - 1;
		m_joinCounts.put(n1, c);
		if (c == 0 && m_joinNodes.contains(n1))
			m_purgeNeeded = true;
		
		c = m_joinCounts.get(n2) - 1;
		m_joinCounts.put(n2, c);
		if (c == 0 && m_joinNodes.contains(n2))
			m_purgeNeeded = true;

		log.debug("purge needed: " + m_purgeNeeded);
	}
	
	public Map<String,Integer> getScores(Graph<QueryNode> queryGraph) {
		Set<Integer> visited = new HashSet<Integer>();
		int startNode = -1;
		final Map<String,Integer> scores = new HashMap<String,Integer>();
		for (int i = 0; i < queryGraph.nodeCount(); i++) {
			String node = queryGraph.getNode(i).getSingleMember();
			if (!node.startsWith("?")) {
				scores.put(node, 0);
				startNode = i;
			}
		}
		
		if (startNode == -1)
			startNode = 0;
		
		Stack<Integer> tov = new Stack<Integer>();
		
		tov.push(startNode);
		
		while (tov.size() > 0) {
			int node = tov.pop();
			
			if (visited.contains(node))
				continue;
			visited.add(node);

			String curNode = queryGraph.getNode(node).getSingleMember();
			
			int min = Integer.MAX_VALUE;
			for (int i : queryGraph.predecessors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			for (int i : queryGraph.successors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			if (!scores.containsKey(curNode))
				scores.put(curNode, min + 1);
		}
		
		return scores;
	}
	
	private void filterTable(GTable<String> table, Set<String> values, int col) {
		long start = System.currentTimeMillis();
		List<String[]> filteredRows = new ArrayList<String[]>();
		for (String[] row : table) {
			if (values.contains(row[col]))
				filteredRows.add(row);
		}
		log.debug("filtered table: " + table.rowCount() + " => " + filteredRows.size() + " in " + (System.currentTimeMillis() - start));
		table.setRows(filteredRows);
	}
	
	private void estimateBenefit(Graph<QueryNode> queryGraph) throws StorageException {
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			String src = getSourceColumn(edge);
			String tgt = getTargetColumn(edge);
			
			int dataCardinality = m_index.getObjectCardinality(edge.getLabel());
			int indexEdgeCount = m_p2to.get(edge.getLabel()).rowCount();
			
			Set<String> subjects = new HashSet<String>(), objects = new HashSet<String>();
			for (String[] row : m_p2to.get(edge.getLabel())) {
				if (!tgt.startsWith("?")) {
					if (!m_es.getExtension(tgt).equals(row[1]))
						continue;
				}
				
				subjects.add(row[0]);
				objects.add(row[1]);
			}
			
			int indexSubjectCardinality = subjects.size();
			int indexObjectCardinality = objects.size();
			
			log.debug(src + "->" + tgt + "(" + Util.truncateUri(edge.getLabel())+ ") dg card: " + dataCardinality + " | ig: " + indexEdgeCount + ", s: " + indexSubjectCardinality + ", o: " + indexObjectCardinality);
		}
	}
	
	public GTable<String> match() throws StorageException {
//		estimateBenefit(m_queryGraph);
		if (m_queryGraph.edgeCount() == 1) {
			return getTable(m_queryGraph.edges().get(0), 1, null);
		}
		
		final Map<String,Integer> scores = getScores(m_queryGraph);
		log.debug(scores);
		
		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(m_queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = m_queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = m_queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = m_queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = m_queryGraph.getNode(e2.getDst()).getSingleMember();
			
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);
				
				if (e1score < e2score)
					return -1;
				else if (e1score > e2score)
					return 1;
				
				boolean g1 = !s1.startsWith("?") || !d1.startsWith("?");
				boolean g2 = !s2.startsWith("?") || !d2.startsWith("?");
				
				if ((g1 && g2) || (!g1 && !g2)) {
					int r1 = m_p2to.get(e1.getLabel()).rowCount();
					int r2 = m_p2to.get(e2.getLabel()).rowCount();
					
					if (r1 < r2)
						return -1;
					else if (r1 > r2)
						return 1;
					else 
						return 0;
				}
				else if (g1)
					return -1;
				else
					return 1;
			}
		});
		
		toVisit.addAll(m_queryGraph.edges());

		GTable<String> result = null;
		List<GTable<String>> extTables = new ArrayList<GTable<String>>();
		List<GTable<String>> tripleTables = new ArrayList<GTable<String>>();

		while (toVisit.size() > 0) {
			long start = System.currentTimeMillis();
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			String sourceCol = getSourceColumn(currentEdge);
			String targetCol = getTargetColumn(currentEdge);
			log.debug(sourceCol + " (" + m_queryGraph.inDegreeOf(currentEdge.getSrc()) + ") -> " + targetCol + " (" + m_queryGraph.inDegreeOf(currentEdge.getDst()) + ") (" + currentEdge.getLabel() + ")");
			
			int sourceIn = m_queryGraph.inDegreeOf(currentEdge.getSrc());
			int targetIn = m_queryGraph.inDegreeOf(currentEdge.getDst());
			m_purgeNeeded = false;
			
			GTable<String> sourceExts = null, targetExts = null;
			for (GTable<String> t : extTables) {
				if (t.hasColumn(sourceCol))
					sourceExts = t;
				if (t.hasColumn(targetCol))
					targetExts = t;
			}
			
			GTable<String> sourceTriples = null, targetTriples = null;
			for (GTable<String> t : tripleTables) {
				if (t.hasColumn(sourceCol))
					sourceTriples = t;
				if (t.hasColumn(targetCol))
					targetTriples = t;
			}
			
			log.debug("property cardinality: " + m_index.getObjectCardinality(currentEdge.getLabel()));
			
			
			if (sourceExts == null && targetExts == null) {
				// lone edge, without any adjoining result area
				GTable<String> tripleTable = new GTable<String>(sourceCol, targetCol);
				result = getTable(currentEdge, 0, tripleTable);
				processed(sourceCol, targetCol);
				
				tripleTables.add(tripleTable);
				
				extTables.add(result);
			}
			else {
				Integer newColumn = null;
//				if (result.hasColumn(sourceCol) && result.hasColumn(targetCol)) {
				if (sourceExts != null && targetExts != null) {
					GTable<String> first, second;
					String firstCol, secondCol;
					GTable<String> currentEdges;
					int currentEdgesCol;
					
					// TODO determine join order by sorted column or only sorting the smaller table
					if (sourceExts.rowCount() < targetExts.rowCount()) {
						first = sourceExts;
						firstCol = sourceCol;
						
						second = targetExts;
						secondCol = targetCol;
						
						currentEdgesCol = 0;
					} 
					else {
						first = targetExts;
						firstCol = targetCol;
						
						second = sourceExts;
						secondCol = sourceCol;
						
						currentEdgesCol = 1;
					}
					
					currentEdges = getTable(currentEdge, currentEdgesCol, null);
					
					if (sourceExts == targetExts) {
						// edge is between two nodes in the same result table
						result = Tables.hashJoin(result, currentEdges, Arrays.asList(sourceCol, targetCol));
						log.debug("hash join");
					}
					else {
						// edge connects two result tables
						log.debug("source triple table: " + sourceTriples);
						log.debug("target triple table: " + targetTriples);
						
						// TODO decide which to prefer, or both?
						if (targetTriples != null && targetTriples.rowCount() > 0) {
							Set<String> extsWithSubjectAsObject = new HashSet<String>();
							for (String[] targetRow : targetTriples) {
								String ext = m_es.getExtension(targetRow[targetTriples.getColumn(targetCol)]);
								extsWithSubjectAsObject.add(ext);
							}
							log.debug(extsWithSubjectAsObject.size());
							
							filterTable(currentEdges, extsWithSubjectAsObject, 1);
						}
						
						if (sourceTriples != null && sourceTriples.rowCount() > 0) {
							if (sourceIn > 0) {
								log.debug("!!!!");
							}
							else {
								Set<String> exts = new HashSet<String>();
								for (String[] sourceRow : sourceTriples)
									exts.addAll(m_es.getExtensions(Index.SE, sourceRow[sourceTriples.getColumn(sourceCol)]));
								log.debug(exts.size());
								
								filterTable(currentEdges, exts, 1);
							}
						}
						
						if (targetTriples != null && sourceTriples != null) {
							GTable<String> table = new GTable<String>(sourceCol, targetCol);
							GTable<String> triples;
							Index index;
							String col;
							if (sourceTriples.rowCount() < targetTriples.rowCount()) {
								triples = sourceTriples;
								index = Index.EPS;
								col = sourceCol;
							}
							else {
								triples = targetTriples;
								index = Index.EPO;
								col = targetCol;
							}
							
							Set<String> exts = new HashSet<String>();
							for (String[] extRow : currentEdges) {
								exts.add(extRow[1]);
							}
							
							log.debug("load ops: " + exts.size() * triples.rowCount());
							
							for (String ext : exts) {
								for (String[] row : triples) {
									table.addRows(m_es.getIndexTable(index, ext, currentEdge.getLabel(), row[triples.getColumn(col)]).getRows());
								}
							}
							
							sourceTriples.sort(sourceCol, true);
							table.sort(sourceCol, true);
							triples = Tables.mergeJoin(sourceTriples, table, sourceCol);
							
							targetTriples.sort(targetCol, true);
							triples.sort(targetCol, true);
							triples = Tables.mergeJoin(targetTriples, triples, targetCol);
							
							tripleTables.remove(sourceTriples);
							tripleTables.remove(targetTriples);
							tripleTables.add(triples);
						}
						
						first.sort(firstCol, true);
						second.sort(secondCol, true);
							
						log.debug(currentEdges + " " + first + " " + second);
						
						result = Tables.mergeJoin(first, currentEdges, firstCol);
						result.sort(secondCol, true);
						result = Tables.mergeJoin(result, second, secondCol);
						
						extTables.remove(sourceExts);
						extTables.remove(targetExts);
						extTables.add(result);
					}
				}
				else if (sourceExts != null) {
					// edge points out of a previous result, i.e. sourceCol is the join column
					GTable<String> currentEdges = getTable(currentEdge, 0, null);

					if (sourceTriples != null && sourceTriples.rowCount() > 0) {
						// there are triples for the source node
//						log.debug("source triple table: " + sourceTriples);
//						
//						Set<String> extsWith = new HashSet<String>(), extsWithout = new HashSet<String>();
//						for (String[] extRow : currentEdges) {
//							if (m_inDegree.get(extRow[0]) > 0)
//								extsWith.add(extRow[0]);
//							else
//								extsWithout.add(extRow[0]);
//						}
//						log.debug(extsWith.size() + " " + extsWithout.size());
//						
//						Set<String> currentTargetExts = new HashSet<String>();
//						GTable<String> currentTriples = new GTable<String>(sourceCol, targetCol);
//						
//						Set<String> validObjectExtensions = new HashSet<String>();
//						for (String[] currentExts : currentEdges) {
//							currentTargetExts.add(currentExts[1]);
//						}
//						log.debug("load ops: " + currentTargetExts.size() * sourceTriples.rowCount());
//						for (String ext : currentTargetExts) {
//							int x = currentTriples.rowCount();
//							for (String[] sourceRow : sourceTriples) {
//								currentTriples.addRows(m_es.getIndexTable(Index.EPS, ext, currentEdge.getLabel(), sourceRow[0]).getRows());
//							}
//							if (currentTriples.rowCount() > x)
//								validObjectExtensions.add(ext);
//						}
//						log.debug(currentTargetExts.size());
//						log.debug(validObjectExtensions.size());
//						
//						currentTriples.sort(0, true);
//						sourceTriples.sort(sourceTriples.getColumn(sourceCol), true);
//						GTable<String> triples = Tables.mergeJoin(sourceTriples, currentTriples, sourceCol);
//						tripleTables.remove(sourceTriples);
//						tripleTables.add(triples);
//						
//						filterTable(currentEdges, validObjectExtensions, 1);
					}
					
					sourceExts.sort(sourceCol, true);
					result = Tables.mergeJoin(sourceExts, currentEdges, sourceCol);
					newColumn = result.getColumn(targetCol);
					
					extTables.remove(sourceExts);
					extTables.add(result);
				}
				else {
					// edge points into a previous result, i.e. targetCol is the join column
					GTable<String> currentEdges = getTable(currentEdge, 1, null);
					
					if (targetTriples != null && targetTriples.rowCount() > 0) {
						// there are triples for the target node
						log.debug("target triple table: " + targetTriples);
						
						Set<String> validSubjectExtensions = new HashSet<String>();
						
						// check for duplicate extensions
						log.debug("load ops: " + targetTriples.rowCount());
						GTable<String> currentTripleTable = new GTable<String>(sourceCol, targetCol);
						for (String[] row : targetTriples) {
							String object = row[targetTriples.getColumn(targetCol)];
							String objectExt = m_es.getExtension(object);
							
							GTable<String> t = m_es.getIndexTable(Index.EPO, objectExt, currentEdge.getLabel(), object);
							for (String[] r : t) {
								currentTripleTable.addRow(r);
								if (m_queryGraph.inDegreeOf(currentEdge.getSrc()) > 0)
									validSubjectExtensions.add(m_es.getExtension(r[0]));
							}
						}
						
						log.debug(currentTripleTable);

						if (m_queryGraph.inDegreeOf(currentEdge.getSrc()) > 0) {
							// node in query graph has incoming edge, use OE index and subjects
							// of current edge as objects of incident edge
							log.debug(validSubjectExtensions);
							filterTable(currentEdges, validSubjectExtensions, 0);
						} else {
							// node in query graph has no incoming edge, use SE index for
							// current target node
							Set<String> targetExtsWithSubject = new HashSet<String>();
							for (String[] curRow : currentTripleTable) {
								targetExtsWithSubject.addAll(m_es.getExtensions(Index.SE, curRow[0]));
							}
							log.debug(targetExtsWithSubject.size());
							filterTable(currentEdges, targetExtsWithSubject, 1);
						}
						
//						currentTripleTable.sort(targetCol, true);
//						targetTriples.sort(targetCol, true);
//						tripleTables.remove(targetTriples);
						
//						tripleTables.add(Tables.mergeJoin(currentTripleTable, targetTriples, targetCol));
					}
					
					targetExts.sort(targetCol, true);
					result = Tables.mergeJoin(targetExts, currentEdges, targetCol);
					newColumn = result.getColumn(sourceCol);
					
					extTables.remove(targetExts);
					extTables.add(result);
				}
				
				processed(sourceCol, targetCol);
				extTables.remove(result);
				if (m_purgeNeeded)
					result = purgeTable(result, newColumn);
				extTables.add(result);
			}
			
			log.debug("rows: " + result.rowCount());
			
			if (result.rowCount() == 0)
				return null;

//			tripleTables.clear();

			log.debug("ext tables: " + extTables);
			log.debug("triple tables: " + tripleTables);
			log.debug("edge time: " + (System.currentTimeMillis() - start));
			log.debug("");
		}
		log.debug("rows: " + result.rowCount());
//		if (m_purgeNeeded)
//			result = purgeTable(result, null);

		return result;
	}
}
