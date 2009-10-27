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

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;
import org.semanticweb.yars.nx.namespace.RDF;

import edu.unika.aifb.graphindex.algorithm.graph.GraphIsomorphism;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.keyword.model.SQueryKeywordElement;
import edu.unika.aifb.graphindex.searcher.structured.sig.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Statistics;
import edu.unika.aifb.graphindex.util.Util;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 10; 
//	private DirectedMultigraph<NodeElement,EdgeElement> m_indexGraph;
	private Map<String,NodeElement> m_nodes;
	private Map<KeywordSegment,List<GraphElement>> m_keywordSegments;
	private Set<String> m_keywords;
	private List<Subgraph> m_subgraphs;
	private int m_k = 10;
	private Map<KeywordSegment,PriorityQueue<Cursor>> m_keywordQueues;
	private Map<String,Set<KeywordSegment>> m_edgeUri2Keywords; 
	private Set<EdgeElement> m_edgesWithCursors;
	private Map<KeywordSegment,Set<NodeElement>> m_ksStartNodes;
	private Map<NodeElement,List<EdgeElement>> m_node2edges;
	private GraphIsomorphism m_iso;
	private Map<String,Double> m_propertyWeights;
	private long m_matchingStart;
	private int m_dataEdges;
	private Set<String> m_dataProperties;
	private Set<EdgeElement> m_addedEdges;
	private Set<NodeElement> m_addedNodes;
	private Map<String,Set<String>> m_nodesWithConcepts;
	
	private long TIMEOUT = 3000;
	
	private static final Logger log = Logger.getLogger(ExploringIndexMatcher.class);
	
	public ExploringIndexMatcher(IndexReader idxReader) throws IOException, StorageException {
		super(idxReader);
	
		m_iso = new GraphIsomorphism();
		
		log.debug(Util.memory());
	}
	
	private void reset() {
		TIMEOUT = 3000;
		
		m_subgraphs = new ArrayList<Subgraph>();
		m_nodesWithConcepts = new HashMap<String,Set<String>>();
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
		m_keywordSegments = new HashMap<KeywordSegment,List<GraphElement>>();
		
		if (m_addedEdges != null) {
			int removed = 0;
			for (NodeElement node : m_node2edges.keySet()) {
				node.reset();
				for (Iterator<EdgeElement> i = m_node2edges.get(node).iterator(); i.hasNext(); ) {
					EdgeElement edge = i.next();
					if (m_dataProperties.contains(edge.getLabel()) || m_addedEdges.contains(edge)) {
						i.remove();
						removed++;
					}
				}
			}
			for (NodeElement node : m_addedNodes)
				m_node2edges.remove(node);
			
			log.debug("removed: " + removed);
		}
		
		m_addedEdges = new HashSet<EdgeElement>();
		m_addedNodes = new HashSet<NodeElement>();
		m_dataEdges = 0;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void initialize() throws StorageException, IOException {
//		m_indexGraph = new DirectedMultigraph<NodeElement,EdgeElement>(EdgeElement.class);
		m_node2edges = new HashMap<NodeElement,List<EdgeElement>>();
		m_nodes = new HashMap<String,NodeElement>();
		
		m_dataProperties = m_idxReader.getDataProperties();

		Map<String,Double> extensionWeights = (Map<String,Double>)Yaml.load(m_idxReader.getIndexDirectory().getFile(IndexDirectory.EXT_WEIGHTS_FILE));
		m_propertyWeights = (Map<String,Double>)Yaml.load(m_idxReader.getIndexDirectory().getFile(IndexDirectory.PROPERTY_FREQ_FILE));
		
		IndexStorage gs = m_idxReader.getStructureIndex().getGraphIndexStorage();

		int graphEdges = 0;
		int reflexiveEdges = 0;
		for (String property : m_idxReader.getObjectProperties()) {
			Table<String> table = gs.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property);
			for (String[] row : table) {
				String src = row[0];
				String trg = row[1];
				
				// ignore reflexive edges, because queries have different semantics in this regard,
				// i.e. a reflexive edge at the sig level does not necessarily translate into a reflexive edges
				// at the data level
				if (src.equals(trg)) {
//					log.debug("reflexive edge: " + row[0] + " " + property + " " + row[1]);
					reflexiveEdges++;
					continue;		 
				}					 
				
				NodeElement source = m_nodes.get(src);
				if (source == null) {
					source = new NodeElement(src);
					source.setCost(1 - extensionWeights.get(src));
					source.setCost(1);
					
					m_nodes.put(src, source);
					m_node2edges.put(source, new ArrayList<EdgeElement>(10));
				}
	
				NodeElement target = m_nodes.get(trg);
				if (target == null) {
					target = new NodeElement(trg);
					target.setCost(1 - extensionWeights.get(trg));
					target.setCost(1);
					
					m_nodes.put(trg, target);
					m_node2edges.put(target, new ArrayList<EdgeElement>(10));
				}
				EdgeElement edge = new EdgeElement(source, property, target);
				edge.setCost(1 - m_propertyWeights.get(property));
				m_node2edges.get(source).add(edge);
				m_node2edges.get(target).add(edge);
				
				graphEdges++;
//				m_indexGraph.addEdge(source, target, new EdgeElement(source, property, target));
			}
		}
		
//		int dataEdges = 0;
//		Map<NodeElement,List<EdgeElement>> toAdd = new HashMap<NodeElement,List<EdgeElement>>();
//		for (NodeElement node : m_node2edges.keySet()) {
//			List<EdgeElement> sourceEdges = m_node2edges.get(node);
//			for (Iterator<String[]> i = m_idxReader.getStructureIndex().getSPIndexStorage().iterator(IndexDescription.EXTDP, new DataField[] { DataField.PROPERTY }, node.getLabel()); i.hasNext(); ) {
//				String[] res = i.next();
//				
//				NodeElement target = new NodeElement("db" + dataEdges);
//				target.setCost(1);
//				EdgeElement edge = new EdgeElement(node, res[0], target);
//				edge.setCost(1 - m_propertyWeights.get(res[0]));
//				
//				List<EdgeElement> targetEdges = new ArrayList<EdgeElement>();
//				targetEdges.add(edge);
//				toAdd.put(target, targetEdges);
//				
//				sourceEdges.add(edge);
//				
//				dataEdges++;
//			}
//		}
//		
//		for (NodeElement node : toAdd.keySet())
//			m_node2edges.put(node, toAdd.get(node));
		
		m_p2ts = null;
		m_p2to = null;
		extensionWeights = null;
		System.gc();
		log.debug(Util.memory());
		
		log.debug("graph edges: " + graphEdges);
		log.debug("reflexive edges ignored: " + reflexiveEdges);
	}
	
	private void addEdges(Set<String> properties) throws StorageException, IOException {
		Map<NodeElement,List<EdgeElement>> toAdd = new HashMap<NodeElement,List<EdgeElement>>();
		for (NodeElement node : m_node2edges.keySet()) {
			List<EdgeElement> sourceEdges = m_node2edges.get(node);
			for (Iterator<String[]> i = m_idxReader.getStructureIndex().getSPIndexStorage().iterator(IndexDescription.EXTDP, new DataField[] { DataField.PROPERTY }, node.getLabel()); i.hasNext(); ) {
				String[] res = i.next();
				
				if (properties.contains(res[0])) {					
					NodeElement target = new NodeElement("db" + m_dataEdges);
					target.setCost(1);
					EdgeElement edge = new EdgeElement(node, res[0], target);
					edge.setCost(1 - m_propertyWeights.get(res[0]));
					if (edge.getCost() == 0.0)
						edge.setCost(0.0001);

					List<EdgeElement> targetEdges = new ArrayList<EdgeElement>();
					targetEdges.add(edge);
					toAdd.put(target, targetEdges);
					
					sourceEdges.add(edge);
					
					m_addedNodes.add(target);
					m_addedEdges.add(edge);
					m_dataEdges++;
				}
			}
		}
		
		for (NodeElement node : toAdd.keySet())
			m_node2edges.put(node, toAdd.get(node));
	}
	
	private EdgeElement addEdge(NodeElement node, String property) {
		for (EdgeElement edge : m_node2edges.get(node)) {
			if (edge.getLabel().equals(property)) 
				return edge;
		}
		
		NodeElement target = new NodeElement("dbe" + m_dataEdges);
		target.setCost(1);
		
		EdgeElement edge = new EdgeElement(node, property, target);
		edge.setCost(1 - m_propertyWeights.get(property));
		if (edge.getCost() == 0)
			edge.setCost(0.0001);
//		edge.setCost(0);
//		edge.setCost(1);
		
		m_node2edges.get(node).add(edge);
		
		List<EdgeElement> targetEdges = new ArrayList<EdgeElement>();
		targetEdges.add(edge);
		
		m_node2edges.put(target, targetEdges);
		
		m_addedNodes.add(target);
		m_addedEdges.add(edge);
		m_dataEdges++;
		
		return edge;
	}
	
	private EdgeElement getEdge(NodeElement node, String property) {
		List<EdgeElement> edges = m_node2edges.get(node);
		for (EdgeElement edge : edges)
			if (edge.getLabel().equals(property))
				return edge;
		return null;
	}
	
	public void setKeywords(Map<KeywordSegment,List<KeywordElement>> keywords, HybridQuery query) throws StorageException, IOException {
		reset();

		log.debug(keywords.keySet());
		for (KeywordSegment keyword : keywords.keySet()) {
			PriorityQueue<Cursor> queue = new PriorityQueue<Cursor>();
			m_keywords.addAll(keyword.getKeywords());
			m_ksStartNodes.put(keyword, new HashSet<NodeElement>());
			
			for (KeywordElement ele : keywords.get(keyword)) {
				Set<KeywordSegment> keywordSet = new HashSet<KeywordSegment>();
				keywordSet.add(keyword);
				
				if (ele.getType() == KeywordElement.CONCEPT) {
					TIMEOUT = 4000;
					
					NodeElement node = m_nodes.get(ele.getUri());
					if (node == null) {
						log.debug("node missing in graph " + ele.getUri());
						continue;
					}
					
					for (EdgeElement edge : m_node2edges.get(node)) {
						if (edge.getLabel().equals(RDF.TYPE.toString()) && edge.getTarget() == node) {
							Cursor start = new NodeCursor(keywordSet, node);
							start.setCost(1);
							Cursor edgeCursor = new EdgeCursor(keywordSet, edge, start);
							edgeCursor.setCost(1);
							
							Cursor nodeCursor = new NodeCursor(keywordSet, edge.getSource(), edgeCursor);
							nodeCursor.setCost(nodeCursor.getCost() - 0.1 * (keyword.getKeywords().size() - 1));
							nodeCursor.setCost(nodeCursor.getCost() / ele.getMatchingScore());

							Set<String> concepts = m_nodesWithConcepts.get(edge.getSource().getLabel());
							if (concepts == null) {
								concepts = new HashSet<String>();
								m_nodesWithConcepts.put(edge.getSource().getLabel(), concepts);
							}
							for (String e : ele.entities)
								concepts.add(e);
							
//							edge.getSource().addCursor(nodeCursor);
							
							queue.add(nodeCursor);
//							log.debug("concept cursor: " + nodeCursor);
						}
					}
				}
				else if (ele.getType() == KeywordElement.ENTITY) {
					// HACK replace NodeElement objects with their equivalent from the graph
					NodeElement node = m_nodes.get(ele.getUri());
					if (node == null) {
						log.debug("node missing in graph " + ele.getUri());
						continue;
					}
//					node.addFrom((NodeElement)ele); // don't forget to copy stuff
					
					if (keyword.getKeywords().contains("STRUCTURED")) {
						SQueryKeywordElement sqele = (SQueryKeywordElement)ele;
						StructuredQueryCursor c = new StructuredQueryCursor(keywordSet, node, query, sqele.getAttachNode());
						c.entities = ele.entities;
//						log.debug(c);
						
						c.addInProperties(ele.getInProperties());
						c.addOutProperties(ele.getOutProperties());

						node.addInProperties(ele.getInProperties());
						node.addOutProperties(ele.getOutProperties());

						c.setInPropertyWeights(ele.getInPropertyWeights());
						c.setOutPropertyWeights(ele.getOutPropertyWeights());
						
//						log.debug(sqele + " " + sqele.entities);
//						log.debug(" " + ele.getInPropertyWeights());
//						log.debug(" " + ele.getOutPropertyWeights());

						queue.add(c);
					}
					else {
						String property = ele.getAttributeUri();
						EdgeElement dataEdge = addEdge(node, property);
						
						if (dataEdge == null) {
							log.warn("data edge missing " + node + " " + property);
							continue;
						}
						

						Cursor start = new NodeCursor(keywordSet, dataEdge.getTarget());
						Cursor edgeCursor = new EdgeCursor(keywordSet, dataEdge, start);
						
						KeywordNodeCursor nodeCursor = new KeywordNodeCursor(keywordSet, node, edgeCursor);
						nodeCursor.setDataIndex(m_idxReader.getDataIndex());
						nodeCursor.m_keywordElement = ele;
						nodeCursor.setCost(nodeCursor.getCost() - 0.1 * (keyword.getKeywords().size() - 1));
						nodeCursor.setCost(nodeCursor.getCost() / ele.getMatchingScore());
					
						nodeCursor.addInProperties(ele.getInProperties());
						nodeCursor.addOutProperties(ele.getOutProperties());
						
						nodeCursor.setInPropertyWeights(ele.getInPropertyWeights());
						nodeCursor.setOutPropertyWeights(ele.getOutPropertyWeights());
						
						node.addInProperties(ele.getInProperties());
						node.addOutProperties(ele.getOutProperties());
						
//						log.debug(nodeCursor + " " + ele.entities);
//						log.debug(" " + ele.getInPropertyWeights());
//						log.debug(" " + ele.getOutPropertyWeights());

//						if (ele.entities.contains("http://dbpedia.org/resource/Freddie_Mercury"))
//							nodeCursor.track = true;
						
//						node.addCursor(nodeCursor);
						
						queue.add(nodeCursor);
//						log.debug(nodeCursor + " " + dataEdge);
					}
					
					m_ksStartNodes.get(keyword).add(node);
				}
				else if (ele.getType() == KeywordElement.RELATION || ele.getType() == KeywordElement.ATTRIBUTE) {
					Set<KeywordSegment> edgeKeywords = m_edgeUri2Keywords.get(ele.getUri());
					if (edgeKeywords == null) {
						edgeKeywords = new HashSet<KeywordSegment>();
						m_edgeUri2Keywords.put(ele.getUri(), edgeKeywords);
					}
					edgeKeywords.add(keyword);
				}
			}

			if (!queue.isEmpty()) {
				m_queues.add(queue);
				m_keywordQueues.put(keyword, queue);
			}
		}
		log.debug("keywords: " + m_keywords);
		
		addEdges(m_edgeUri2Keywords.keySet());
		
		Set<String> nodeKeywords = new HashSet<String>();
		Set<String> edgeKeywords = new HashSet<String>();
		for (KeywordSegment ks : m_keywordQueues.keySet())
			nodeKeywords.addAll(ks.getKeywords());
		for (KeywordSegment ks : keywords.keySet())
			if (!m_keywordQueues.containsKey(ks))
				edgeKeywords.addAll(ks.getKeywords());
		
		// add edge keyword elements to permitted next edges in the cursors
		for (PriorityQueue<Cursor> queue : m_queues) {
			for (Cursor c : queue) {
				c.addOutProperties(m_edgeUri2Keywords.keySet());
				c.addInProperties(m_edgeUri2Keywords.keySet());
				
				for (String uri : m_edgeUri2Keywords.keySet()) {
					c.addInPropertyWeight(uri, 0);
					c.addOutPropertyWeight(uri, 0);
				}
			}
		}
		
//		log.debug("node keywords: " + nodeKeywords);
//		log.debug("edge keywords: " + edgeKeywords);
//		log.debug(m_ksStartNodes);
		
		m_counters.set(Counters.KWQUERY_NODE_KEYWORDS, nodeKeywords.size());
		m_counters.set(Counters.KWQUERY_EDGE_KEYWORDS, edgeKeywords.size());
		m_counters.set(Counters.KWQUERY_KEYWORDS, m_keywords.size());

//		m_keywordSegments = keywords;
		
		setMaxDistance(nodeKeywords.size() * 2 + m_edgeUri2Keywords.size());
		log.debug("nodeKeywords: " + nodeKeywords);
		log.debug("max distance: " + m_maxDistance);
		log.debug("data edges added: " + m_dataEdges);
	}
	
	public void setK(int k) {
		m_k = k;
	}
	
	public void setMaxDistance(int distance) {
		m_maxDistance = distance + 2;
	}

	@Override
	protected boolean isCompatibleWithIndex() {
		return true;
	}
	
	private boolean topK(GraphElement currentElement) {
		if (currentElement.getKeywords().size() == m_keywords.size()) {
			// current element is a connecting element

//			Statistics.start(this, Statistics.Timing.EX_TOPK_COMBINATIONS);
			Set<List<Cursor>> combinations = currentElement.getCursorCombinations(m_keywords);
//			Statistics.end(this, Statistics.Timing.EX_TOPK_COMBINATIONS);

//			Statistics.inc(this, Statistics.Counter.EX_TOPK_COMBINATIONS, combinations.size());
			
//			Statistics.start(this, Statistics.Timing.EX_TOPK_SUBGRAPH);
			for (List<Cursor> combination : combinations) {
//				Statistics.start(this, Statistics.Timing.EX_TOPK_SUBGRAPH_CREATION);
				Subgraph sg = new Subgraph(new HashSet<Cursor>(combination));
//				Statistics.end(this, Statistics.Timing.EX_TOPK_SUBGRAPH_CREATION);

				if (sg.isValid() && !m_subgraphs.contains(sg)) {// && !sg.hasDanglingEdge()) {
					boolean found = false;
					for (Iterator<Subgraph> i = m_subgraphs.iterator(); i.hasNext(); ) {
//					for (Subgraph existing : m_subgraphs) {
						Subgraph existing = i.next();
						try {
//							Statistics.start(this, Statistics.Timing.EX_TOPK_SUBGRAPH_ISO);
							List<Map<String,String>> maps = m_iso.getIsomorphicMappings(existing, sg);
//							Statistics.end(this, Statistics.Timing.EX_TOPK_SUBGRAPH_ISO);

							if (maps.size() > 0) {
								found = true;
//								existing.addMappings(maps);
								
								if (existing.getCost() > sg.getCost()) {
									// replace existing if score of new is lower
//									if (sg.track)
//										log.debug("iso rem" + sg);
									i.remove();
									found = false;
								}
//								else if (sg.track)
//									log.debug("iso " + sg);
								
								break;
							}
						} catch (Exception e) {
							found = true;
						}
					}
					
					if (!found) {
//						if (sg.track)
//							log.debug("add " + sg);
						m_subgraphs.add(sg);
						Collections.sort(m_subgraphs);
						for (int i = m_subgraphs.size() - 1; i >= m_k; i--)
							m_subgraphs.remove(i);
					}
				}
			}
//			log.debug(m_subgraphs.size());
//			Statistics.end(this, Statistics.Timing.EX_TOPK_SUBGRAPH);
		}
		
		if (System.currentTimeMillis() - m_matchingStart > TIMEOUT * 1.2) {
			Collections.sort(m_subgraphs);
			if (m_subgraphs.size() >= m_k)
				for (int i = m_subgraphs.size() - 1; i >= m_k; i--)
					m_subgraphs.remove(i);
			return true;
		}
		
		if (m_subgraphs.size() < m_k)
			return false;

		Collections.sort(m_subgraphs);
		
		for (int i = m_subgraphs.size() - 1; i >= m_k; i--)
			m_subgraphs.remove(i);

		if (m_queues.peek() == null || m_queues.peek().peek() == null)
			return true;
		
		double highestCost = m_subgraphs.get(m_subgraphs.size() - 1).getCost();
		double lowestCost = m_queues.peek().peek().getCost();
		
//		log.debug(m_subgraphs.get(m_subgraphs.size() - 1).edgeSet().size() + " "  + m_queues.peek().peek().getEdges().size());
//		log.debug(highestCost + " " + lowestCost + " " + m_queues.peek().peek()); 
		
		if (highestCost <= lowestCost) {
			log.debug("topk reached");
			return true;
		}
		
		if (System.currentTimeMillis() - m_matchingStart > TIMEOUT) {
			log.debug("topk");
			return true;
		}
		
		return false;
	}
	
	public void match() throws StorageException {
		int i = 0;
		int expansions = 0;
		m_matchingStart = System.currentTimeMillis();
		while (m_queues.size() > 0) {
			boolean done = false;
			PriorityQueue<Cursor> cursorQueue = m_queues.poll();
			Cursor minCursor = cursorQueue.peek();
			GraphElement currentElement = minCursor.getGraphElement();

			if (minCursor.getDistance() <= m_maxDistance) {
				currentElement.addCursor(minCursor);

				if (minCursor.getDistance() < m_maxDistance - 1) {
					Set<GraphElement> parents = minCursor.getParents();
					KeywordSegment startKS = null;
					for (KeywordSegment ks : minCursor.getStartCursor().getKeywordSegments()) {
						startKS = ks;
						break;
					}
					
//					Statistics.start(this, Statistics.Timing.EX_NEIGHBORS);
					List<GraphElement> neighbors = currentElement.getNeighbors(m_node2edges, minCursor, m_edgeUri2Keywords.keySet());
//					Statistics.end(this, Statistics.Timing.EX_NEIGHBORS);

//					int size = neighbors.size();
//					long start = System.currentTimeMillis();
//					Map<String,List<GraphElement>> labels = new HashMap<String,List<GraphElement>>();
//					for (GraphElement neighbor : neighbors) {
//						List<GraphElement> labelNeighbors = labels.get(neighbor.getLabel());
//						if (labelNeighbors == null) {
//							labelNeighbors = new ArrayList<GraphElement>();
//							labels.put(neighbor.getLabel(), labelNeighbors);
//						}
//						labelNeighbors.add(neighbor);
//					}
//					
//					neighbors = new ArrayList<GraphElement>();
//					for (String label : labels.keySet()) {
//						List<GraphElement> labelNeighbors = labels.get(label);
//						Collections.sort(labelNeighbors, new Comparator<GraphElement>() {
//							public int compare(GraphElement o1, GraphElement o2) {
//								return ((Double)o1.getCost()).compareTo(o2.getCost());
//							}
//						});
//						
//						for (int j = 0; j < Math.min(labelNeighbors.size(), 5); j++)
//							neighbors.add(labelNeighbors.get(j));
//					}
//					
//					log.debug(size + " " + neighbors.size() + " " + labels.size() + " " + (System.currentTimeMillis() - start));
//					log.debug(neighbors.size());
					
					for (GraphElement neighbor : neighbors) {
						if (!parents.contains(neighbor) && !m_ksStartNodes.get(startKS).contains(neighbor)) {
//							Statistics.start(this, Statistics.Timing.EX_NEXTCURSOR);
							Cursor c = minCursor.getNextCursor(neighbor, m_nodesWithConcepts);
//							Statistics.end(this, Statistics.Timing.EX_NEXTCURSOR);
							
							if (c == null)
								continue;
							
							cursorQueue.add(c);
							expansions++;

							// if a cursor crosses an edge whose property was matched to one or more keyword
							// segments, add these segments to the new cursor
							// only the last cursor will have complete information about which segments are covered,
							// i.e. the cursor at the connecting element
							// (this is by design as a cursor can be parent to multiple other cursors, which will not
							// necessarily cover the same segments later on)
							if (neighbor instanceof EdgeElement && m_edgeUri2Keywords.containsKey(neighbor.getLabel())) { 
								for (KeywordSegment ks : m_edgeUri2Keywords.get(neighbor.getLabel()))
									c.addKeywordSegment(ks);
								
								c.setCost(c.getCost() - 0.1);
								
								c.getGraphElement().addCursor(c);
								
								done = done || topK(c.getGraphElement());
							}
						}
//						else
//							log.debug("already visited");
					}
				}
				
				done = done || topK(currentElement);
				
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
		
		log.debug("expansions: " + expansions);
		
	}
	
	public List<TranslatedQuery> indexMatches(StructuredQuery sq, Map<String,Set<QNode>> ext2var) {
		List<TranslatedQuery> queries = new ArrayList<TranslatedQuery>();
		for (Subgraph sg : m_subgraphs) {
			log.debug(sg);
			queries.addAll(sg.attachQuery(sq, ext2var));
		}
		return queries;
	}
}
