package edu.unika.aifb.graphindex.query.matcher_v2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.AbstractIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.EvaluationClass;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.DataField;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class SmallIndexMatchesValidator extends AbstractIndexMatchesValidator {
	private IndexDescription m_idxESPS;
	private IndexDescription m_idxESPO;
	private IndexDescription m_idxPOES;
	private IndexDescription m_idxPSES;

	private static final Logger log = Logger.getLogger(SmallIndexMatchesValidator.class);

	public SmallIndexMatchesValidator(StructureIndex index, StatisticsCollector collector) {
		super(index, collector);
	}
	
	protected boolean isCompatibleWithIndex() {
		m_idxESPS = m_index.getCompatibleIndex(DataField.EXT_SUBJECT, DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT);
		m_idxESPO = m_index.getCompatibleIndex(DataField.EXT_SUBJECT, DataField.PROPERTY, DataField.OBJECT, DataField.SUBJECT);
		m_idxPOES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		m_idxPSES = m_index.getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT);
//		m_idxEOPO = m_index.getCompatibleIndex(DataField.EXT_OBJECT, DataField.PROPERTY, DataField.OBJECT, DataField.SUBJECT);
		
		if (m_idxESPS == null || m_idxESPO == null || m_idxPOES == null)
			return false;
		
		return true;
	}

	public List<String[]> validateIndexMatches(Query query, final Graph<QueryNode> queryGraph, GTable<String> indexMatches, List<String> selectVariables) throws StorageException {
		final Map<String,Integer> scores = query.calculateConstantProximities();
		for (String label : scores.keySet())
			if (scores.get(label) > 1)
				scores.put(label, 10);
		
		log.debug("constant proximities: " + scores);

		final Set<String> matchedNodes = new HashSet<String>();

		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		EvaluationClass evc = new EvaluationClass(indexMatches);
		classes.add(evc);

		final Map<String,Integer> matchCardinalities = evc.getCardinalityMap(new HashSet<String>());//query.getRemovedNodes());
		for (String node : matchCardinalities.keySet())
			if (Util.isConstant(node))
				matchCardinalities.put(node, 1);
		log.debug("match cardinalities: " + matchCardinalities);

		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
			
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);
				
				// order first by proximity to a constant
				if (e1score < e2score)
					return -1;
				else if (e1score > e2score)
					return 1;
				
				int c1 = matchCardinalities.get(s1) * matchCardinalities.get(d1);
				int c2 = matchCardinalities.get(s2) * matchCardinalities.get(d2);

				if (c1 == c2) {
					Integer ce1 = m_index.getObjectCardinality(e1.getLabel());
					Integer ce2 = m_index.getObjectCardinality(e2.getLabel());
					
					if (matchCardinalities.get(d1) == matchCardinalities.get(d2) && ce1 != null && ce2 != null && ce1.intValue() != ce2.intValue()) {
						if (ce1 < ce2)
							return 1;
						else
							return -1;
					}
					
					if (matchCardinalities.get(d1) < matchCardinalities.get(d2))
						return 1;
					else
						return -1;
				}
				else if (c1 < c2)
					return -1;
				else
					return 1;
			}
		});
		
		toVisit.addAll(queryGraph.edges());

		// disable merge join logging
		Tables.log.setLevel(Level.OFF);
		
		log.debug("");
		while (toVisit.size() > 0) {
			long start = System.currentTimeMillis();
			
			GraphEdge<QueryNode> currentEdge;
			String srcLabel, trgLabel, property;
			List<GraphEdge<QueryNode>> skipped = new ArrayList<GraphEdge<QueryNode>>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);

				property = currentEdge.getLabel();
				srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
				trgLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			}
			while (!matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel));
			
			skipped.remove(currentEdge);
			toVisit.addAll(skipped);
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ") (classes: " + classes.size() + ")");
			
			Map<String,List<EvaluationClass>> ext2ec = new HashMap<String,List<EvaluationClass>>();
			if (!matchedNodes.contains(srcLabel) && matchedNodes.contains(trgLabel)) {
				// cases 1 a,d: edge has one unprocessed node, the source
				updateClasses(classes, srcLabel, srcLabel, ext2ec);
				
				classes = evaluateTargetMatched(currentEdge, property, srcLabel, trgLabel, classes, ext2ec);
			}
			else if (matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel)) {
				// cases 1 b,c: edge has one unprocessed node, the target
				updateClasses(classes, trgLabel, srcLabel, ext2ec);
				
				classes = evaluateSourceMatched(currentEdge, property, srcLabel, trgLabel, classes, ext2ec);
			}
			else if (!matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel)) {
				// case 2: edge has two unprocessed nodes
				updateClasses(classes, trgLabel, null, null);
				updateClasses(classes, srcLabel, srcLabel, ext2ec);
				
				classes = evaluateUnmatched(currentEdge, property, srcLabel, trgLabel, classes, ext2ec);
			}
			else {
				// case 3: both nodes already processed
				ext2ec = getValueMap(classes, srcLabel); // no updateClasses, have to generate explicitly
				classes = evaluateBothMatched(currentEdge, property, srcLabel, trgLabel, classes, ext2ec);
			}
			
			matchedNodes.add(srcLabel);
			matchedNodes.add(trgLabel);
			
			log.debug("classes: " + classes.size());
			log.debug("time: " + (System.currentTimeMillis() - start));
			log.debug("");
		}
		
		List<String[]> result = new ArrayList<String[]>();
		Set<String> sigs = new HashSet<String>();
		int rows = 0;
		for (EvaluationClass ec : classes) {
			if (ec.getResults().size() == 0)
				continue;
//			result.addAll(ec.getResults().get(0).getTable());
			GTable<String> table = ec.getResults().get(0);
			rows += table.rowCount();
			
			int[] cols = new int [selectVariables.size()];
			for (int i = 0; i < selectVariables.size(); i++)
				cols[i] = table.getColumn(selectVariables.get(i));
					
			for (String[] row : ec.getResults().get(0).getTable()) {
				String[] selectRow = new String [cols.length];
				StringBuilder sb = new StringBuilder();
				
				for (int i = 0; i < cols.length; i++) {
					selectRow[i] = row[cols[i]];
					sb.append(row[cols[i]]).append("__");
				}
				
				String sig = sb.toString();
				if (!sigs.contains(sig)) {
					sigs.add(sig);
					result.add(selectRow);
				}
			}
		}
		log.debug("rows: " + rows + " => " + result.size());
		
		return result;
	}
	
	// case 1 a,d: target node is matched, source is not
	private List<EvaluationClass> evaluateTargetMatched(GraphEdge<QueryNode> edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec) throws StorageException {
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();

		boolean filterUsingTarget = true;
		int filteredRows = 0, totalRows = 0;
		
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> targetTable = ec.findResult(trgLabel);
				int trgCol = targetTable.getColumn(trgLabel);
				ec.getResults().remove(targetTable);
				
				if (filterUsingTarget) {
//					log.debug("filter: " + targetTable.rowCount() + " => " + filteredTable.rowCount());
					GTable<String> filteredTable = filterTable(m_idxPOES, targetTable, srcExt, property, trgLabel);
					filteredRows += targetTable.rowCount() - filteredTable.rowCount();
					totalRows += targetTable.rowCount();
					targetTable = filteredTable;
					
					if (targetTable.rowCount() == 0)
						continue;
				}
				
				GTable<String> table = new GTable<String>(srcLabel, trgLabel);
				
				Set<String> values = new HashSet<String>();
				for (String[] trgRow : targetTable) {
					if (!values.contains(trgRow[trgCol])) {
						table.addRows(m_es.getIndexTable(m_idxESPO, srcExt, property, trgRow[trgCol]).getRows());
						values.add(trgRow[trgCol]);
					}
				}
				
				if (table.rowCount() == 0)
					continue;
				
				targetTable.sort(trgLabel, true);
				table.sort(trgLabel, true);
				table = Tables.mergeJoin(targetTable, table, trgLabel);
				
				if (table.rowCount() > 0) {
					ec.getResults().add(table);
					remainingClasses.add(ec);
				}
			}
		}
		
		if (filterUsingTarget)
			log.debug("filtered rows: " + filteredRows + "/" + totalRows);
		
		return remainingClasses;
	}

	// case 1 b,c: source node is matched, target is not
	private List<EvaluationClass> evaluateSourceMatched(GraphEdge<QueryNode> edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec) throws StorageException {
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();

		boolean filterUsingSource = true;

		int filteredRows = 0, totalRows = 0;
		
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> sourceTable = ec.findResult(srcLabel);
				int srcCol = sourceTable.getColumn(srcLabel);
				ec.getResults().remove(sourceTable);
				
				if (filterUsingSource) {
					GTable<String> filteredTable = filterTable(m_idxPSES, sourceTable, srcExt, property, srcLabel);
					filteredRows += sourceTable.rowCount() - filteredTable.rowCount();
					totalRows += sourceTable.rowCount();
					sourceTable = filteredTable;
					
					if (sourceTable.rowCount() == 0)
						continue;
				}
				
				GTable<String> table = new GTable<String>(srcLabel, trgLabel);
				
				Set<String> values = new HashSet<String>();
				for (String[] srcRow : sourceTable) {
					if (!values.contains(srcRow[srcCol])) {
						table.addRows(m_es.getIndexTable(m_idxESPS, srcExt, property, srcRow[srcCol]).getRows());
						values.add(srcRow[srcCol]);
					}
				}
				
				if (table.rowCount() == 0)
					continue;
				
				sourceTable.sort(srcLabel, true);
				table.sort(srcLabel, true);
				table = Tables.mergeJoin(sourceTable, table, srcLabel);
				
				if (table.rowCount() > 0) {
					ec.getResults().add(table);
					remainingClasses.add(ec);
				}
			}
		}
		
		if (filterUsingSource)
			log.debug("filtered rows: " + filteredRows + "/" + totalRows);
		
		return remainingClasses;
	}

	// case 2: neither node is matched
	private List<EvaluationClass> evaluateUnmatched(GraphEdge<QueryNode> edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec) throws StorageException {
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();
		if (Util.isConstant(trgLabel)) {
			// use ESPO index to load triples
			for (String srcExt : ext2ec.keySet()) {
				GTable<String> table = m_es.getIndexTable(m_idxESPO, srcExt, property, trgLabel);
				if (table.rowCount() == 0)
					continue;
				
				table.setColumnName(0, srcLabel);
				table.setColumnName(1, trgLabel);

				for (EvaluationClass ext : ext2ec.get(srcExt)) {
					ext.getResults().add(table);
					remainingClasses.add(ext);
				}
			}
			return remainingClasses;
		}
		else {
			throw new UnsupportedOperationException("edges with two variables and both unprocessed should not happen");
		}
	}

	// TODO handle sourceTable == targetTable (hash join or new multi-column merge join)
	private List<EvaluationClass> evaluateBothMatched(GraphEdge<QueryNode> edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec) throws StorageException {
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();
		
		boolean filter = true;
		int filteredRows = 0, totalRows = 0;
		
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> sourceTable = ec.findResult(srcLabel);
				ec.getResults().remove(sourceTable);
				
				GTable<String> targetTable = ec.findResult(trgLabel);
				ec.getResults().remove(targetTable);
				
				GTable<String> table = new GTable<String>(srcLabel, trgLabel);
				Set<String> values = new HashSet<String>();
				
				IndexDescription index;
				GTable<String> prevTable;
				int col;
				
				if (sourceTable.rowCount() < targetTable.rowCount()) {
					index = m_idxESPS;
					prevTable = sourceTable;
					col = sourceTable.getColumn(srcLabel);
					
					if (filter) {
						GTable<String> filteredTable = filterTable(m_idxPSES, sourceTable, srcExt, property, srcLabel);
						filteredRows += sourceTable.rowCount() - filteredTable.rowCount();
						totalRows += sourceTable.rowCount();
						
						sourceTable = filteredTable;
						if (sourceTable.rowCount() == 0)
							continue;
					}
				}
				else {
					index = m_idxESPO;
					prevTable = targetTable;
					col = targetTable.getColumn(trgLabel);

					if (filter) {
						GTable<String> filteredTable = filterTable(m_idxPOES, targetTable, srcExt, property, trgLabel);
						filteredRows += targetTable.rowCount() - filteredTable.rowCount();
						totalRows += targetTable.rowCount();
					
						targetTable = filteredTable;
						if (targetTable.rowCount() == 0)
							continue;
					}
				}
				
				for (String[] row : prevTable) {
					if (!values.contains(row[col])) {
						table.addRows(m_es.getIndexTable(index, srcExt, property, row[col]).getRows());
						values.add(row[col]);
					}
				}
				
				if (table.rowCount() == 0)
					continue;
				
				sourceTable.sort(srcLabel, true);
				table.sort(srcLabel, true);
				table = Tables.mergeJoin(sourceTable, table, srcLabel);
				
				targetTable.sort(trgLabel, true);
				table.sort(trgLabel);
				table = Tables.mergeJoin(targetTable, table, trgLabel);
				
				if (table.rowCount() > 0) {
					ec.getResults().add(table);
					remainingClasses.add(ec);
				}
			}
		}

		if (filter)
			log.debug("filtered rows: " + filteredRows + "/" + totalRows);
		
		return remainingClasses;
	}
	
	private GTable<String> filterTable(IndexDescription index, GTable<String> table, String ext, String property, String colName) throws StorageException {
		GTable<String> filteredTable = new GTable<String>(table, false);
		int col = table.getColumn(colName);
		for (String[] trgRow : table) {
			if (m_es.getDataSet(index, property, trgRow[col]).contains(ext))
				filteredTable.addRow(trgRow);
		}
//		log.debug("filtered: " + table.rowCount() + " => " + filteredTable.rowCount());
		return filteredTable;
	}

	private void updateClasses(List<EvaluationClass> classes, String key, String valueMapNode, Map<String,List<EvaluationClass>> valueMap) {
		t.start(Timings.UC);
		long start = System.currentTimeMillis();
		
		int x = classes.size();
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		for (EvaluationClass ec : classes)
			newClasses.addAll(ec.addMatch(key, Util.isConstant(key), valueMapNode, valueMap));
		classes.addAll(newClasses);
		
		log.debug("update classes: " + x + " -> " + classes.size() + " in " + (System.currentTimeMillis() - start) + (valueMap != null ? ", value map: " + valueMap.keySet().size() : ""));
		
		t.end(Timings.UC);
	}

	private Map<String,List<EvaluationClass>> getValueMap(List<EvaluationClass> classes, String node) {
		Map<String,List<EvaluationClass>> val2ec = new HashMap<String,List<EvaluationClass>>();
		for (EvaluationClass ec : classes) {
			String val = ec.getMatch(node);
			if (!val2ec.containsKey(val))
				val2ec.put(val, new ArrayList<EvaluationClass>());
			val2ec.get(val).add(ec);
		}
		log.debug("distinct exts for " + node + ": " + val2ec.keySet().size());
		return val2ec;
	}

	public void clearCaches() {
	}
}
