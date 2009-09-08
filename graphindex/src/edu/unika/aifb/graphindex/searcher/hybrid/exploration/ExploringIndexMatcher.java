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
import java.util.Iterator;
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
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.structured.sig.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

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
	private Map<KeywordSegment,Set<NodeElement>> m_ksStartNodes;
	
	private static final Logger log = Logger.getLogger(ExploringIndexMatcher.class);
	
	public ExploringIndexMatcher(IndexReader idxReader) throws IOException, StorageException {
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
		m_ksStartNodes = new HashMap<KeywordSegment,Set<NodeElement>>();
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
			m_ksStartNodes.put(keyword, new HashSet<NodeElement>());
			for (GraphElement ele : keywords.get(keyword)) {
				Set<KeywordSegment> keywordSet = new HashSet<KeywordSegment>();
				keywordSet.add(keyword);
				
				if (ele instanceof NodeElement) {
					// HACK replace NodeElement objects with their equivalent from the graph
					NodeElement node = m_nodes.get(ele.getLabel());
					node.addFrom((NodeElement)ele); // don't forget to copy stuff
					
					if (keyword.getKeywords().contains("STRUCTURED"))
						queue.add(new StructuredQueryCursor(keywordSet, node));
					else
						queue.add(new NodeCursor(keywordSet, node));
					
					m_ksStartNodes.get(keyword).add(node);
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
//					Set<NodeElement> nodes = new HashSet<NodeElement>();
//					for (NodeElement node : ((StructuredMatchElement)ele).getNodes()) {
//						// HACK replace NodeElement objects with their equivalent from the graph
//						NodeElement n = m_nodes.get(node.getLabel()); 
//						n.addFrom(node); // don't forget to copy stuff
//						
//						queue.add(new StructuredQueryCursor(keywordSet, ele, n));
//						nodes.add(n);
//					}
//					((StructuredMatchElement)ele).setNodes(nodes);
					NodeElement node = m_nodes.get(((StructuredMatchElement)ele).getNode().getLabel());
					node.addFrom(((StructuredMatchElement)ele).getNode());
					((StructuredMatchElement)ele).setNode(node);
					queue.add(new StructuredQueryCursor(keywordSet, ele));
				}
			}
//			log.debug("queue size for " + keyword + ": " + queue.size());
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
		
//		log.debug("node keywords: " + nodeKeywords);
//		log.debug("edge keywords: " + edgeKeywords);
//		log.debug(m_ksStartNodes);
		
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
				Subgraph sg = new Subgraph(new HashSet<Cursor>(combination));

				if (!m_subgraphs.contains(sg) && !sg.hasDanglingEdge()) {
					boolean found = false;
					for (Subgraph existing : m_subgraphs) {
						try {
							List<Map<NodeElement,NodeElement>> maps = existing.isIsomorphicTo(sg);
//							for (Iterator<Map<String,String>> i = maps.iterator(); i.hasNext(); ) {
//							Map<String,String> map = i.next();
//							if (!map.get(existing.getStructuredNode().getLabel()).equals(existing.getStructuredNode().getLabel()))
//								i.remove();
//							}
							if (maps.size() > 0) {
								found = true;
								existing.addMappings(maps);
								break;
							}
						} catch (Exception e) {
							log.debug(e);
						}
					}
					
					if (!found)
						m_subgraphs.add(sg);
				}
			}
//			log.debug(m_subgraphs.size());
		}
		
		if (m_subgraphs.size() < m_k)
			return false;

		Collections.sort(m_subgraphs);
		
		for (int i = m_subgraphs.size() - 1; i >= m_k; i--)
			m_subgraphs.remove(i);

		if (m_queues.peek() == null || m_queues.peek().peek() == null)
			return true;
		
		int highestCost = m_subgraphs.get(m_subgraphs.size() - 1).getCost();
		int lowestCost = m_queues.peek().peek().getCost();
		
//		log.debug(m_subgraphs.get(m_subgraphs.size() - 1).edgeSet().size() + " "  + m_queues.peek().peek().getEdges().size());
//		log.debug(highestCost + " " + lowestCost + " " + m_queues.peek().peek()); 
		
		if (highestCost <= lowestCost) {
			log.debug("topk reached");
			return true;
		}
		
		return false;
	}
	
	public void match() throws StorageException {
		int i = 0;
		while (m_queues.size() > 0) {
			PriorityQueue<Cursor> cursorQueue = m_queues.poll();
			Cursor minCursor = cursorQueue.peek();
			GraphElement currentElement = minCursor.getGraphElement();

			if (minCursor.getDistance() < m_maxDistance) {
				currentElement.addCursor(minCursor);

				if (minCursor.getDistance() < m_maxDistance - 1) {
					Set<GraphElement> parents = minCursor.getParents();
					KeywordSegment startKS = null;
					for (KeywordSegment ks : minCursor.getStartCursor().getKeywordSegments()) {
						startKS = ks;
						break;
					}
					
					List<GraphElement> neighbors = currentElement.getNeighbors(m_indexGraph, minCursor);
					for (GraphElement neighbor : neighbors) {
						if (!parents.contains(neighbor) && !m_ksStartNodes.get(startKS).contains(neighbor)) {
							Cursor c = minCursor.getNextCursor(neighbor);
							cursorQueue.add(c);

							// if a cursor crosses an edge whose property was matched to one or more keyword
							// segments, add these segments to the new cursor
							// only the last cursor will have complete information about which segments are covered,
							// i.e. the cursor at the connecting element
							// (this is by design as a cursor can be parent to multiple other cursors, which will not
							// necessarily cover the same segments later on)
							if (neighbor instanceof EdgeElement && m_edgeUri2Keywords.containsKey(neighbor.getLabel())) 
								for (KeywordSegment ks : m_edgeUri2Keywords.get(neighbor.getLabel()))
									c.addKeywordSegment(ks);
						}
//						else
//							log.debug("already visited");
					}
				}
				
				boolean done = topK(currentElement);
				
				if (done)
					break;
				
			}
			
			cursorQueue.remove(minCursor);
			
			if (!cursorQueue.isEmpty())
				m_queues.add(cursorQueue);
			
			i++;
//			if (i % 1000 == 0) {
//				String s = "";
//				for (KeywordSegment ks : m_keywordQueues.keySet())
//					s += ks.toString() + ": " + (m_keywordQueues.get(ks).peek() != null ? m_keywordQueues.get(ks).peek().getCost() : "x") + "/" + m_keywordQueues.get(ks).size() + ", ";
//				log.debug(s + " " + m_subgraphs.size());
//			}
		}
	}
	
	public List<TranslatedQuery> indexMatches(StructuredQuery sq, Map<String,Set<QNode>> ext2var) {
		List<TranslatedQuery> queries = new ArrayList<TranslatedQuery>();
		for (Subgraph sg : m_subgraphs) {
//			log.debug(sg);
			queries.addAll(sg.attachQuery(sq, ext2var));
		}
		return queries;
	}
}
