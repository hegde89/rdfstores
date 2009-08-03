package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.Cursor;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.GraphElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.NodeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.Subgraph;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.structured.sig.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;

public class HybridExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 10; 
	private DirectedMultigraph<NodeElement,EdgeElement> m_indexGraph;
	private Map<String,NodeElement> m_nodes;
	private Map<KeywordSegment,List<GraphElement>> m_keywordSegments;
	private Set<String> m_keywords;
	private List<Subgraph> m_subgraphs;
	private int m_k = 10;
	private Map<KeywordSegment,PriorityQueue<Cursor>> m_keywordQueues;
	private Map<String,Set<KeywordSegment>> m_edgeUri2Keywords; 
	private Set<EdgeElement> m_edgesWithCursors;
	
	private static final Logger log = Logger.getLogger(HybridExploringIndexMatcher.class);
	
	public HybridExploringIndexMatcher(IndexReader idxReader) throws IOException {
		super(idxReader);
	
		m_subgraphs = new ArrayList<Subgraph>();
		m_nodes = new HashMap<String,NodeElement>();
		m_queues = new PriorityQueue<PriorityQueue<Cursor>>(20, new Comparator<PriorityQueue<Cursor>>() {
			public int compare(PriorityQueue<Cursor> o1, PriorityQueue<Cursor> o2) {
				if (o1.peek() == null && o2.peek() == null)
					return 0;
				else if (o1.peek() == null && o2.peek() != null)
					return 1;
				else if (o1.peek() != null && o2.peek() == null)
					return -1;
				return o1.peek().compareTo(o2.peek());
			}
		});
		m_keywordQueues = new HashMap<KeywordSegment,PriorityQueue<Cursor>>();
		m_edgeUri2Keywords = new HashMap<String,Set<KeywordSegment>>();
		m_edgesWithCursors = new HashSet<EdgeElement>();
		m_keywords = new HashSet<String>();
	}
	
	@Override
	public void initialize() throws StorageException, IOException {
		m_indexGraph = new DirectedMultigraph<NodeElement,EdgeElement>(EdgeElement.class);
		
		IndexStorage gs = m_idxReader.getStructureIndex().getGraphIndexStorage();

		for (String property : m_idxReader.getObjectProperties()) {
			Table<String> table = gs.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property);
			for (String[] row : table) {
				String src = row[0];
				String trg = row[1];
				
				NodeElement source = m_nodes.get(src);
				if (source == null) {
					source = new NodeElement(src);
					m_indexGraph.addVertex(source);
					m_nodes.put(src, source);
				}
	
				NodeElement target = m_nodes.get(trg);
				if (target == null) {
					target = new NodeElement(trg);
					m_indexGraph.addVertex(target);
					m_nodes.put(trg, target);
				}
				
				m_indexGraph.addEdge(source, target, new EdgeElement(source, property, target));
			}
		}
		
		log.debug("ig edges: " + m_indexGraph.edgeSet().size());
		log.debug("ig nodes: " + m_indexGraph.vertexSet().size());
	}
	
	public void setKeywords(Map<KeywordSegment,List<GraphElement>> keywords) {
//		log.debug(keywords);
		for (KeywordSegment keyword : keywords.keySet()) {
			PriorityQueue<Cursor> queue = new PriorityQueue<Cursor>();
			m_keywords.addAll(keyword.getKeywords());
			for (GraphElement ele : keywords.get(keyword)) {
				if (ele instanceof NodeElement) {
					NodeElement node = m_nodes.get(ele.getLabel());
					queue.add(new Cursor(keyword, node, null, 0));
				}
				else if (ele instanceof EdgeElement) {
					Set<KeywordSegment> edgeKeywords = m_edgeUri2Keywords.get(ele.getLabel());
					if (edgeKeywords == null) {
						edgeKeywords = new HashSet<KeywordSegment>();
						m_edgeUri2Keywords.put(ele.getLabel(), edgeKeywords);
					}
					edgeKeywords.add(keyword);
				}
				else if (ele instanceof StructuredMatchElement) {
					Set<NodeElement> nodes = new HashSet<NodeElement>();
					for (NodeElement node : ((StructuredMatchElement)ele).getNodes()) {
						NodeElement n = m_nodes.get(node.getLabel());
						queue.add(new StructuredQueryCursor(keyword, ele, null, 0, n));
						nodes.add(n);
					}
					((StructuredMatchElement)ele).setNodes(nodes);
				}
			}
			log.debug("queue size for " + keyword + ": " + queue.size());
			if (!queue.isEmpty()) {
				m_queues.add(queue);
				m_keywordQueues.put(keyword, queue);
			}
		}
		log.debug("keywords: " + m_keywords);
		
		Set<String> nodeKeywords = new HashSet<String>();
		Set<String> edgeKeywords = new HashSet<String>();
		for (KeywordSegment ks : m_keywordQueues.keySet())
			nodeKeywords.addAll(ks.getKeywords());
		for (KeywordSegment ks : keywords.keySet())
			if (!m_keywordQueues.containsKey(ks))
				edgeKeywords.addAll(ks.getKeywords());
		log.debug("node keywords: " + nodeKeywords);
		log.debug("edge keywords: " + edgeKeywords);
		
		m_counters.set(Counters.KWQUERY_NODE_KEYWORDS, nodeKeywords.size());
		m_counters.set(Counters.KWQUERY_EDGE_KEYWORDS, edgeKeywords.size());
		m_counters.set(Counters.KWQUERY_KEYWORDS, m_keywords.size());

		m_keywordSegments = keywords;
		
		setMaxDistance(nodeKeywords.size() * 2);
		log.debug("max distance: " + m_maxDistance);
	}
	
	public void setK(int k) {
		m_k = k;
	}
	
	public void setMaxDistance(int distance) {
		m_maxDistance = distance;
	}

	@Override
	protected boolean isCompatibleWithIndex() {
		return true;
	}
	
	private boolean topK(GraphElement currentElement) {
		if (currentElement.getKeywords().size() == m_keywords.size()) {
			// current element is a connecting element
//			log.debug(m_subgraphs.size());
			List<List<Cursor>> combinations = currentElement.getCursorCombinations();
			for (List<Cursor> combination : combinations) {
//				Set<String> combinationKeywords = new HashSet<String>();
//				for (Cursor c : combination)
//					for (KeywordSegement ks : c.getKeywordSegments())
//						combinationKeywords.addAll(ks.getKeywords());
//				
//				if (!combinationKeywords.equals(m_keywords)) {
//					log.debug(combination);
//					continue;
//				}
				Subgraph sg = new Subgraph(new HashSet<Cursor>(combination));

				if (!m_subgraphs.contains(sg)) {
					if (!sg.hasDanglingEdge())
						m_subgraphs.add(sg);
				}
			}
//			log.debug(m_subgraphs.size());
		}
		
		if (m_subgraphs.size() < m_k)
			return false;

		Collections.sort(m_subgraphs);
		
		for (int i = m_subgraphs.size() - 1; i > m_k; i--)
			m_subgraphs.remove(i);

		if (m_queues.peek() == null || m_queues.peek().peek() == null)
			return true;
		
		int highestCost = m_subgraphs.get(m_subgraphs.size() - 1).getCost();
		int lowestCost = m_queues.peek().peek().getCost();
		
//		log.debug(highestCost + " " + lowestCost); 
		
		if (highestCost < lowestCost) {
			log.debug("done");
			return true;
		}
		
		return false;
	}
	
	public void match() throws StorageException {
//		int edgeCursorsStarted = 0;
		while (m_queues.size() > 0) {
			PriorityQueue<Cursor> cursorQueue = m_queues.poll();
			Cursor minCursor = cursorQueue.poll();
			GraphElement currentElement = minCursor.getGraphElement();

			if (minCursor.getDistance() < m_maxDistance) {
				currentElement.addCursor(minCursor);

				if (minCursor.getDistance() < m_maxDistance - 1) {
					Set<GraphElement> parents = minCursor.getParents();
					
					List<GraphElement> neighbors = currentElement.getNeighbors(m_indexGraph, minCursor);
					for (GraphElement neighbor : neighbors) {
						if (!parents.contains(neighbor)) {
							if (neighbor instanceof EdgeElement && m_edgeUri2Keywords.containsKey(neighbor.getLabel())) {
								Cursor c = new Cursor(minCursor.getKeywordSegments(), neighbor, minCursor, minCursor.getCost() + 1);
								for (KeywordSegment ks : m_edgeUri2Keywords.get(neighbor.getLabel()))
									c.addKeywordSegment(ks);
								cursorQueue.add(c);
							}
							else {
								if (minCursor instanceof StructuredQueryCursor)
									cursorQueue.add(new StructuredQueryCursor(minCursor.getKeywordSegments(), neighbor, minCursor, minCursor.getCost() + 1, ((StructuredQueryCursor)minCursor).getStartNode()));
								else
									cursorQueue.add(new Cursor(minCursor.getKeywordSegments(), neighbor, minCursor, minCursor.getCost() + 1));
							}
						}

					}
				}
				
				boolean done = topK(currentElement);
				
				if (done)
					break;
				
			}
			
			if (!cursorQueue.isEmpty())
				m_queues.add(cursorQueue);
//			String s = "";
//			for (KeywordSegement ks : m_keywordQueues.keySet())
//				s += ks.toString() + ": " + m_keywordQueues.get(ks).size() + ", ";
//			log.debug(s);
			
//			String s = "";
//			for (PriorityQueue<Cursor> q : m_queues)
//				s += q.size() + " ";
//			log.debug(s);
		}
		
//		for (Subgraph sg : m_subgraphs) {
//			sg.toQuery(true);
//		}
	}
	
	public void indexMatches(List<Table<String>> indexMatches, List<StructuredQuery> queries, List<Map<String,Set<KeywordSegment>>> select2ks, boolean withAttributes) {
		List<List<Subgraph>> groups = new ArrayList<List<Subgraph>>();
		
		for (Subgraph sg : m_subgraphs) {
			boolean found = false;
			for (int i = 0; i < groups.size(); i++) {
				List<Subgraph> list = groups.get(i);
				List<Map<String,String>> mappings = list.get(0).isIsomorphicTo(sg);
				if (mappings.size() > 0) {
					list.add(sg);
					
					Map<String,String> vars = list.get(0).getVariableMapping();
					Table<String> table = indexMatches.get(i);
					
					for (Map<String,String> mapping : mappings) {
						String[] row = new String [table.columnCount()];
						
						for (int j = 0; j < table.columnCount(); j++) {
							String colName = table.getColumnName(j);
							row[j] = mapping.get(vars.get(colName));
						}
						
						table.addRow(row);
					}
					
					found = true;
					break;
				}
			}
			
			if (!found) {
//				for (Cursor c : sg.getCursors()) {
//					log.debug(c + " " + c.getStartCursor());
//				}
//				log.debug(sg.edgeSet().size());
				log.debug(sg.edgeSet());
//				log.debug("");
				List<Subgraph> list = new ArrayList<Subgraph>();
				list.add(sg);
				groups.add(list);
				
				Table<String> table = new Table<String>(sg.getQueryNodes());
				String[] row = new String[table.columnCount()];
				for (String col : table.getColumnNames()) {
					row[table.getColumn(col)] = sg.getVariableMapping().get(col);
				}
				table.addRow(row);
				indexMatches.add(table);
				
//				queries.add(sg.toQuery(withAttributes));
				
				Map<String,Set<KeywordSegment>> ksMap = sg.getKSMapping();
				Map<KeywordSegment,KeywordSegment> replace = new HashMap<KeywordSegment,KeywordSegment>();
				
				Set<KeywordSegment> querySegments = new HashSet<KeywordSegment>();
				for (Set<KeywordSegment> ksSet : ksMap.values())
					for (KeywordSegment ks : ksSet)
						querySegments.add(ks);
				
				Set<String> compared = new HashSet<String>();
				for (KeywordSegment ks1 : querySegments) {
					for (KeywordSegment ks2 : querySegments) {
						if (ks1 == ks2)
							continue;
						if (compared.contains(ks1.toString() + "|||" + ks2.toString()))
							continue;
						if (compared.contains(ks2.toString() + "|||" + ks1.toString()))
							continue;
						compared.add(ks1.toString() + "|||" + ks2.toString());
						
						Set<String> ks1keywords = new HashSet<String>(ks1.getKeywords());
						ks1keywords.retainAll(ks2.getKeywords());
						
						if (ks1keywords.size() > 0) {//< ks1.getKeywords().size()) {
							Set<String> remaining = new HashSet<String>(ks1.getKeywords());
							remaining.removeAll(ks1keywords);
							for (KeywordSegment ks : m_keywordSegments.keySet()) {
								if (ks.getKeywords().equals(remaining)) {
									replace.put(ks1, ks);
									break;
								}
							}
						}
					}
				}
//				log.debug(replace);
				for (Set<KeywordSegment> ksSet : ksMap.values()) {
					for (KeywordSegment ks : replace.keySet())
						if (ksSet.contains(ks)) {
							ksSet.remove(ks);
							ksSet.add(replace.get(ks));
						}
							
				}
//				log.debug(ksMap);
				select2ks.add(ksMap);
				
				StructuredQuery q = new StructuredQuery(null);
				
				for (EdgeElement e : sg.edgeSet()) {
					String src = sg.getLabels().get(e.getSource().getLabel());
					String dst = sg.getLabels().get(e.getTarget().getLabel());
					q.addEdge(src, e.getLabel(), dst);
				}
				
				if (withAttributes) {
					int x = 0;
					for (String selectNode : ksMap.keySet()) {
						for (KeywordSegment ks : ksMap.get(selectNode))
							for (String keyword : ks.getKeywords())
								q.addEdge(selectNode, "???" + x++, keyword);
					}
				}
				
				for (QNode var : q.getVariables())
					q.setAsSelect(var.getLabel());
				
				queries.add(q);
 			}
		}
		
//		log.debug(groups);
//		for (List<Subgraph> list : groups)
//			log.debug(list.size());
		log.debug(m_subgraphs.size() + " => " + groups.size());
//		log.debug(indexMatches);
//		for (GTable<String> table : indexMatches)
//			log.debug(table.toDataString());
	}
}
