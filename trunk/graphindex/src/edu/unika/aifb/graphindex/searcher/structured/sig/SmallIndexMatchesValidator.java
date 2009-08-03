package edu.unika.aifb.graphindex.searcher.structured.sig;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.searcher.entity.EntitySearcher;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class SmallIndexMatchesValidator extends AbstractIndexMatchesValidator {
	private IndexStorage m_is;
	
	private IndexDescription m_idxPSESO;
	private IndexDescription m_idxPOESS;
	private IndexDescription m_idxPOES;
	private IndexDescription m_idxSES;
	
	private EntitySearcher m_entitySearcher;
	private List<QueryEdge> m_deferredTypeEdges;

	private static final Logger log = Logger.getLogger(SmallIndexMatchesValidator.class);

	public SmallIndexMatchesValidator(IndexReader idxReader) throws IOException {
		super(idxReader);
		m_is = idxReader.getStructureIndex().getSPIndexStorage();
	}
	
	protected boolean isCompatibleWithIndex() throws IOException {
		m_idxPSESO = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT, DataField.OBJECT);
		m_idxPOESS = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT, DataField.SUBJECT);
		m_idxPOES = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		m_idxSES = m_idxReader.getStructureIndex().getCompatibleIndex(DataField.SUBJECT, DataField.EXT_SUBJECT);
		
		if (m_idxPSESO == null || m_idxPOESS == null || m_idxPOES == null || m_idxSES == null)
			return false;
		
		return true;
	}
	
	public void setIncrementalState(EntitySearcher es, List<QueryEdge> deferredTypeEdges) {
		m_entitySearcher = es;
		m_deferredTypeEdges = deferredTypeEdges;
	}

	public void validateIndexMatches() throws StorageException, IOException {
		PrunedQuery prunedQuery = m_qe.getPrunedQuery();
		final QueryGraph prunedQueryGraph = m_qe.getPrunedQueryGraph();
		List<String> selectVariables = prunedQuery.getSelectVariableLabels();
		
		final Map<String,Integer> scores = m_qe.getProximities();
		for (String label : scores.keySet())
			if (scores.get(label) > 1)
				scores.put(label, 10);
		
		log.debug("constant proximities: " + scores);

		final Set<String> matchedNodes = new HashSet<String>();


		List<EvaluationClass> classes = m_qe.getEvaluationClasses();
//		List<EvaluationClass> prevClasses = m_qe.getEvaluationClasses();
//		classes = null;
		EvaluationClass evc = new EvaluationClass(m_qe.getIndexMatches());
		if (classes == null || classes.size() == 0) {
			classes = new ArrayList<EvaluationClass>();
			classes.add(evc);
		}
			
		
		final Map<String,Integer> matchCardinalities = evc.getCardinalityMap(new HashSet<String>());//query.getRemovedNodes());
		for (String node : matchCardinalities.keySet())
			if (Util.isConstant(node))
				matchCardinalities.put(node, 1);
		log.debug("match cardinalities: " + matchCardinalities);
		evc = null;
		
		final List<QueryEdge> deferred = m_deferredTypeEdges;

		PriorityQueue<QueryEdge> toVisit = new PriorityQueue<QueryEdge>(prunedQueryGraph.edgeCount(), new Comparator<QueryEdge>() {
			public int compare(QueryEdge e1, QueryEdge e2) {
				String s1 = e1.getSource().getLabel();
				String s2 = e2.getSource().getLabel();
				String d1 = e1.getTarget().getLabel();
				String d2 = e2.getTarget().getLabel();
				
				if (deferred != null) {
					if (deferred.contains(e1) && !deferred.contains(e2))
						return 1;
					else if (!deferred.contains(e1) && deferred.contains(e2))
						return -1;
				}
			
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
					Integer ce1 = m_idxReader.getObjectCardinality(e1.getLabel());
					Integer ce2 = m_idxReader.getObjectCardinality(e2.getLabel());
					
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
		
		List<QueryEdge> edges;
		if (m_qe.getVisited().size() > 0) {
			edges = m_qe.toVisit();
			for (QueryEdge edge : m_qe.getVisited()) {
				matchedNodes.add(edge.getSource().getLabel());
				matchedNodes.add(edge.getTarget().getLabel());
			}
		}
		else
			edges = new ArrayList<QueryEdge>(prunedQueryGraph.edgeSet());
		
		toVisit.addAll(edges);
		
		log.debug("remaining edges: " + toVisit.size() + "/" + prunedQueryGraph.edgeCount());
		
		if (toVisit.size() == 0) {
			log.debug("no remaining edges, verifying...");
			List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();
//			for (String node : sourcesOfRemoved) {
//				Map<String,List<EvaluationClass>> ext2ec = new HashMap<String,List<EvaluationClass>>();
//				updateClasses(classes, node, node, ext2ec);
//				
//				for (EvaluationClass ec : classes) {
//					String srcExt = ec.getMatch(node);
//					
//					GTable<String> table = ec.findResult(node);
//					ec.getResults().remove(table);
//					
//					GTable<String> t2 = new GTable<String>(table, false);
//					
//					int col = table.getColumn(node);
//					for (String [] row : table) {
//						if (m_is.getDataSet(m_idxSES, row[col]).contains(srcExt)) {
//							t2.addRow(row);
//						}
//					}
//					
//					if (t2.rowCount() > 0) {
//						ec.getResults().add(t2);
//						remainingClasses.add(ec);
//					}
//				}
//			}
		}
		

		// disable merge join logging
//		Tables.log.setLevel(Level.OFF);
		
//		m_counters.set(Counters.DM_REM_NODES, prunedQuery.getRemovedNodes().size());
//		m_counters.set(Counters.DM_PROCESSED_EDGES, toVisit.size());
		
		log.debug("");
		while (toVisit.size() > 0) {
			long start = System.currentTimeMillis();
			
			QueryEdge currentEdge;
			String srcLabel, trgLabel, property;
			List<QueryEdge> skipped = new ArrayList<QueryEdge>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);

				property = currentEdge.getLabel();
				srcLabel = currentEdge.getSource().getLabel();
				trgLabel = currentEdge.getTarget().getLabel();
			}
			while ((!matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel) && Util.isVariable(srcLabel) && Util.isVariable(trgLabel)));
//				|| (Util.isConstant(trgLabel) && !matchedNodes.contains(srcLabel) && matchedNodes.contains(trgLabel)));
			
			skipped.remove(currentEdge);
			toVisit.addAll(skipped);
			
			log.debug(srcLabel + " -> " + trgLabel + " (" + property + ") (classes: " + classes.size() + ")");
			
			if (m_entitySearcher != null && deferred.contains(currentEdge)) {
				log.debug("edge is rdf:type and deferred, skip normal processing");
				if (!matchedNodes.contains(srcLabel))
					throw new UnsupportedOperationException("deferred edge should have processed node...");
				
				for (EvaluationClass ec : classes) {
					GTable<String> ecTable = ec.findResult(srcLabel);
					ec.getResults().remove(ecTable);
					
					GTable<String> table = new GTable<String>(ecTable, false);
					int col = ecTable.getColumn(srcLabel);
					for (String[] row : ecTable) {
						if (m_entitySearcher.isType(row[col], trgLabel))
							table.addRow(row);
					}
					
//					log.debug("entity filter: " + ecTable.rowCount() + " => " + table.rowCount());
					ec.getResults().add(table);
				}
			}
			else {
				Map<String,List<EvaluationClass>> ext2ec = new HashMap<String,List<EvaluationClass>>();
				if (!matchedNodes.contains(srcLabel) && matchedNodes.contains(trgLabel) && !Util.isConstant(trgLabel)) {
					// cases 1 a,d: edge has one unprocessed node, the source
					updateClasses(classes, srcLabel, srcLabel, ext2ec);
					
					classes = evaluateTargetMatched(prunedQuery, currentEdge, property, srcLabel, trgLabel, classes, ext2ec, false);
				}
				else if (matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel)) {
					// cases 1 b,c: edge has one unprocessed node, the target
					updateClasses(classes, trgLabel, srcLabel, ext2ec);
					
					classes = evaluateSourceMatched(prunedQuery, currentEdge, property, srcLabel, trgLabel, classes, ext2ec, false);
				}
				else if (!matchedNodes.contains(srcLabel) && !matchedNodes.contains(trgLabel) || (!matchedNodes.contains(srcLabel) && Util.isConstant(trgLabel))) {
					// case 2: edge has two unprocessed nodes
					updateClasses(classes, trgLabel, null, null);
					updateClasses(classes, srcLabel, srcLabel, ext2ec);
					
					classes = evaluateUnmatched(prunedQuery, currentEdge, property, srcLabel, trgLabel, classes, ext2ec);
				}
				else {
					// case 3: both nodes already processed
					ext2ec = getValueMap(classes, srcLabel); // no updateClasses, have to generate explicitly
					classes = evaluateBothMatched(prunedQuery, currentEdge, property, srcLabel, trgLabel, classes, ext2ec, false);
				}
			}
			
			matchedNodes.add(srcLabel);
			matchedNodes.add(trgLabel);
			
			log.debug("classes: " + classes.size());
			log.debug("time: " + (System.currentTimeMillis() - start));
			log.debug("");
		}
		
		for (EvaluationClass ec : classes) {
			if (ec.getResults().size() == 0)
				continue;
//			log.debug(ec.getMappings().toDataString());
			m_qe.addResult(ec.getResults().get(0), true);
		}
	}
	
	// case 1 a,d: target node is matched, source is not
	private List<EvaluationClass> evaluateTargetMatched(PrunedQuery prunedQuery, QueryEdge edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec, boolean filterUsingTarget) throws StorageException {
		log.debug("target matched");
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();

		int filteredRows = 0, totalRows = 0;
		
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> targetTable = ec.findResult(trgLabel);
				int trgCol = targetTable.getColumn(trgLabel);
				ec.getResults().remove(targetTable);
				
				String trgExt = ec.getMatch(trgLabel);

//				if (filterUsingTarget) {
//					GTable<String> filteredTable = filterTable(m_idxPOES, targetTable, srcExt, property, trgLabel);
//					filteredRows += targetTable.rowCount() - filteredTable.rowCount();
//					totalRows += targetTable.rowCount();
//					targetTable = filteredTable;
//										
//					if (targetTable.rowCount() == 0)
//						continue;
//				}
				
				GTable<String> table = new GTable<String>(srcLabel, trgLabel);
				
				Set<String> values = new HashSet<String>();
				for (String[] trgRow : targetTable) {
					if (!values.contains(trgRow[trgCol])) {
						GTable<String> t2 = m_is.getIndexTable(m_idxPOESS, DataField.SUBJECT, DataField.OBJECT, property, trgRow[trgCol], srcExt);
						if (prunedQuery.isRootOfPrunedPart(trgLabel)) {
							for (String[] row : t2) {
								if (m_is.getDataItem(m_idxSES, DataField.EXT_SUBJECT, row[1]).equals(trgExt)) 
									table.addRow(row);
							}
						}
						else
							table.addRows(t2.getRows());
						
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
	private List<EvaluationClass> evaluateSourceMatched(PrunedQuery prunedQuery, QueryEdge edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec, boolean filterUsingSource) throws StorageException {
		log.debug("source matched");
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();

		int filteredRows = 0, totalRows = 0;
		
		boolean targetConstant = Util.isConstant(trgLabel);
		log.debug("target constant: " + targetConstant);
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> sourceTable = ec.findResult(srcLabel);
				int srcCol = sourceTable.getColumn(srcLabel);
				ec.getResults().remove(sourceTable);
				
				// the target is the source of pruned edge, verify that the current target extension
				// indeed has triples
				String trgExt = ec.getMatch(trgLabel);
//				if (sourcesOfRemoved.contains(trgLabel) && !m_es.hasTriples(m_idxESPS, trgExt, removedProperties.get(trgLabel), null))
//					continue;

				GTable<String> table = new GTable<String>(srcLabel, trgLabel);

				Set<String> values = new HashSet<String>();
				for (String[] srcRow : sourceTable) {
					if (!values.contains(srcRow[srcCol])) {
						GTable<String> t2 = m_is.getIndexTable(m_idxPSESO, DataField.SUBJECT, DataField.OBJECT, property, srcRow[srcCol], srcExt);
						
						if (!targetConstant)
							table.addRows(t2.getRows());
						else
							for (String[] row : t2)
								if (row[1].equals(trgLabel))
									table.addRow(row);
						
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
	private List<EvaluationClass> evaluateUnmatched(PrunedQuery prunedQuery, QueryEdge edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec) throws StorageException {
		log.debug("both unmatched");
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();
		if (Util.isConstant(trgLabel)) {
			// use ESPO index to load triples
			for (String srcExt : ext2ec.keySet()) {
				GTable<String> table = m_is.getIndexTable(m_idxPOESS, DataField.SUBJECT, DataField.OBJECT, property, trgLabel, srcExt);
				if (table.rowCount() == 0)
					continue;
				
				if (prunedQuery.isRootOfPrunedPart(srcLabel)) {
					GTable<String> t2 = new GTable<String>(table, false);
					for (String[] row : table) {
						if (m_is.getDataItem(m_idxSES, DataField.EXT_SUBJECT, row[0]).equals(srcExt)) 
							t2.addRow(row);
					}
					table = t2;
				}

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
	private List<EvaluationClass> evaluateBothMatched(PrunedQuery prunedQuery, QueryEdge edge, String property, String srcLabel, String trgLabel, List<EvaluationClass> classes, Map<String,List<EvaluationClass>> ext2ec, boolean filter) throws StorageException {
		log.debug("both matched");
		
		List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();
		
		int filteredRows = 0, totalRows = 0;
		
		for (String srcExt : ext2ec.keySet()) {
			for (EvaluationClass ec : ext2ec.get(srcExt)) {
				GTable<String> sourceTable = ec.findResult(srcLabel);
				GTable<String> targetTable = ec.findResult(trgLabel);

				ec.getResults().remove(sourceTable);
				ec.getResults().remove(targetTable);

				GTable<String> table = new GTable<String>(srcLabel, trgLabel);
				Set<String> values = new HashSet<String>();
				
				IndexDescription index;
				GTable<String> prevTable;
				int col;
				DataField indexDF;
				
				if (sourceTable.rowCount() < targetTable.rowCount()) {
					index = m_idxPSESO;
					prevTable = sourceTable;
					col = sourceTable.getColumn(srcLabel);
					indexDF = DataField.SUBJECT;
				}
				else {
					index = m_idxPOESS;
					prevTable = targetTable;
					col = targetTable.getColumn(trgLabel);
					indexDF = DataField.OBJECT;
				}
				
				String trgExt = ec.getMatch(trgLabel);
				
				for (String[] row : prevTable) {
					if (!values.contains(row[col])) {
						GTable<String> t2 = m_is.getIndexTable(index, DataField.SUBJECT, DataField.OBJECT, property, row[col], srcExt);
						t2 = m_is.getTable(index, new DataField[] { DataField.SUBJECT, DataField.OBJECT }, index.createValueArray(DataField.PROPERTY, property, indexDF, row[col], DataField.EXT_SUBJECT, srcExt));
						if (prunedQuery.isRootOfPrunedPart(trgLabel)) {
							for (String[] t2row : t2) {
								if (m_is.getDataItem(m_idxSES, DataField.EXT_SUBJECT, t2row[1]).equals(trgExt))
									table.addRow(t2row);
							}
						}
						else if (srcLabel.equals("?x1")) {
							for (String[] t2row : t2)
								if (m_is.getDataItem(m_idxSES, DataField.EXT_SUBJECT, t2row[0]).equals(srcExt))
									table.addRow(t2row);
						}
						else
							table.addRows(t2.getRows());
						
						values.add(row[col]);
					}
				}

				if (table.rowCount() == 0)
					continue;
				
				if (sourceTable == targetTable) {
					table = Tables.mergeJoin(sourceTable, table, Arrays.asList(srcLabel, trgLabel));
//					table = Tables.hashJoin(sourceTable, table, Arrays.asList(srcLabel, trgLabel));
				}
				else {
					sourceTable.sort(srcLabel, true);
					table.sort(srcLabel, true);
					table = Tables.mergeJoin(sourceTable, table, srcLabel);
					
					targetTable.sort(trgLabel, true);
					table.sort(trgLabel);
					table = Tables.mergeJoin(targetTable, table, trgLabel);
				}

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
	
//	private GTable<String> filterTable(IndexDescription index, GTable<String> table, String ext, String property, String colName) throws StorageException {
//		t.start(Timings.DM_FILTER);
//		GTable<String> filteredTable = new GTable<String>(table, false);
//		int col = table.getColumn(colName);
//		for (String[] trgRow : table) {
//			if (m_is.getDataSet(index, property, trgRow[col]).contains(ext))
//				filteredTable.addRow(trgRow);
//		}
//		t.end(Timings.DM_FILTER);
//		return filteredTable;
//	}

	private void updateClasses(List<EvaluationClass> classes, String key, String valueMapNode, Map<String,List<EvaluationClass>> valueMap) {
		t.start(Timings.DM_CLASSES);
		long start = System.currentTimeMillis();
		
		int x = classes.size();
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		for (EvaluationClass ec : classes)
			newClasses.addAll(ec.addMatch(key, Util.isConstant(key), valueMapNode, valueMap));
		classes.addAll(newClasses);
		
		log.debug("update classes: " + x + " -> " + classes.size() + " in " + (System.currentTimeMillis() - start) + (valueMap != null ? ", value map: " + valueMap.keySet().size() : ""));
		
		t.end(Timings.DM_CLASSES);
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
