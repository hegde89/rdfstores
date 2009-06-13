package edu.unika.aifb.graphindex.query.exploring;

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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.query.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 4; 
	private DirectedMultigraph<NodeElement,EdgeElement> m_indexGraph;
	private Map<String,NodeElement> m_nodes;
	private Map<String,List<GraphElement>> m_keywords;
	private List<Subgraph> m_subgraphs;
	private int m_k = 10;
	private Map<String,PriorityQueue<Cursor>> m_keywordQueues;
	private Map<String,Set<String>> m_edgeUri2Keywords; 
	private Set<EdgeElement> m_edgesWithCursors;
	
	private static final Logger log = Logger.getLogger(ExploringIndexMatcher.class);
	
	public ExploringIndexMatcher(StructureIndex index, String graphName) {
		super(index, graphName);
	
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
		m_keywordQueues = new HashMap<String,PriorityQueue<Cursor>>();
		m_edgeUri2Keywords = new HashMap<String,Set<String>>();
		m_edgesWithCursors = new HashSet<EdgeElement>();
	}
	
	@Override
	public void initialize() throws StorageException {
		m_indexGraph = new DirectedMultigraph<NodeElement,EdgeElement>(EdgeElement.class);
		
		Set<LabeledEdge<String>> edges = m_gs.loadEdges(m_graphName);
		for (LabeledEdge<String> edge : edges) {
			NodeElement source = m_nodes.get(edge.getSrc());
			if (source == null) {
				source = new NodeElement(edge.getSrc());
				m_indexGraph.addVertex(source);
				m_nodes.put(edge.getSrc(), source);
			}

			NodeElement target = m_nodes.get(edge.getDst());
			if (target == null) {
				target = new NodeElement(edge.getDst());
				m_indexGraph.addVertex(target);
				m_nodes.put(edge.getDst(), target);
			}
			
			m_indexGraph.addEdge(source, target, new EdgeElement(source, edge.getLabel(), target));
		}
		
		log.debug("ig edges: " + m_indexGraph.edgeSet().size());
		log.debug("ig nodes: " + m_indexGraph.vertexSet().size());
	}
	
	public void setKeywords(Map<String,List<GraphElement>> keywords) {
		for (String keyword : keywords.keySet()) {
			PriorityQueue<Cursor> queue = new PriorityQueue<Cursor>();
			for (GraphElement ele : keywords.get(keyword)) {
				if (ele instanceof NodeElement) {
					NodeElement node = m_nodes.get(ele.getLabel());
					queue.add(new Cursor(keyword, node, null, 0));
				}
				else if (ele instanceof EdgeElement) {
					Set<String> edgeKeywords = m_edgeUri2Keywords.get(ele.getLabel());
					if (edgeKeywords == null) {
						edgeKeywords = new HashSet<String>();
						m_edgeUri2Keywords.put(ele.getLabel(), edgeKeywords);
					}
					edgeKeywords.add(keyword);
				}
			}
			if (!queue.isEmpty()) {
				m_queues.add(queue);
				m_keywordQueues.put(keyword, queue);
			}
		}
		m_keywords = keywords;
		log.debug(m_keywords);
		log.debug(m_queues);
		log.debug(m_edgeUri2Keywords);
	}
	
	public void setK(int k) {
		m_k = k;
	}

	@Override
	protected boolean isCompatibleWithIndex() {
		return true;
	}
	
	private boolean topK(GraphElement currentElement) {
		if (currentElement.getKeywords().size() == m_keywords.size()) {
			// current element is a connecting element
			List<List<Cursor>> combinations = currentElement.getCursorCombinations();
			for (List<Cursor> combination : combinations) {
				Subgraph sg = new Subgraph(combination);

				if (!m_subgraphs.contains(sg))
					m_subgraphs.add(sg);
			}
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
		
//		log.debug(m_queues.peek().size() + " " + highestCost + " " + lowestCost);
		
		if (highestCost < lowestCost) {
			log.debug("done");
			return true;
		}
		return false;
	}
	
	public void match() throws StorageException {
		int edgeCursorsStarted = 0;
		while (m_queues.size() > 0) {
			PriorityQueue<Cursor> cursorQueue = m_queues.poll();
			Cursor minCursor = cursorQueue.poll();
			GraphElement currentElement = minCursor.getGraphElement();
			
			if (minCursor.getDistance() < m_maxDistance) {
//				log.debug("min cursor: " + minCursor);
				currentElement.addCursor(minCursor);
//				if (currentElement.getKeywords().size() > 2)
//					log.debug(currentElement.getKeywords());
//				if (m_edgesWithCursors.contains(currentElement))
//					log.debug(currentElement);

				if (minCursor.getDistance() < m_maxDistance - 1) {
					List<GraphElement> neighbors = currentElement.getNeighbors(m_indexGraph, minCursor);
					Set<GraphElement> parents = minCursor.getParents();
					for (GraphElement neighbor : neighbors) {
						if (!parents.contains(neighbor))
							cursorQueue.add(new Cursor(minCursor.getKeyword(), neighbor, minCursor, minCursor.getCost() + 1));
						if (neighbor instanceof EdgeElement && !m_edgesWithCursors.contains(neighbor)) {
							Set<String> edgeKeywords = m_edgeUri2Keywords.get(neighbor.getLabel());
							if (edgeKeywords != null) {
								for (String keyword : edgeKeywords) {
									PriorityQueue<Cursor> q = m_keywordQueues.get(keyword);
									if (q == null) {
										q = new PriorityQueue<Cursor>();
										m_queues.add(q);
										m_keywordQueues.put(keyword, q);
									}
									Cursor p = new Cursor(keyword, currentElement, null, 0);
									p.setFakeStart(true);
									currentElement.addCursor(p);
									Cursor c = new Cursor(keyword, neighbor, p, p.getCost() + 1);
									q.add(c);
									edgeCursorsStarted++;
								}
								m_edgesWithCursors.add((EdgeElement)neighbor);
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
//			for (PriorityQueue<Cursor> q : m_queues)
//				s += q.size() + " ";
//			log.debug(s);
		}
		
//		for (Subgraph sg : m_subgraphs) {
//			sg.toQuery(true);
//		}
	}
	
	public void indexMatches(List<GTable<String>> indexMatches, List<Query> queries) {
		List<List<Subgraph>> groups = new ArrayList<List<Subgraph>>();
		
		for (Subgraph sg : m_subgraphs) {
			boolean found = false;
			for (int i = 0; i < groups.size(); i++) {
				List<Subgraph> list = groups.get(i);
				Map<String,String> mapping = list.get(0).isIsomorphicTo(sg);
				if (mapping.size() > 0) {
					list.add(sg);
					
					Map<String,String> vars = list.get(0).getVariableMapping();
					GTable<String> table = indexMatches.get(i);
					String[] row = new String [table.columnCount()];
					
					for (int j = 0; j < table.columnCount(); j++) {
						String colName = table.getColumnName(j);
						row[j] = mapping.get(vars.get(colName));
					}
					
					table.addRow(row);
					
					found = true;
					break;
				}
			}
			
			if (!found) {
				List<Subgraph> list = new ArrayList<Subgraph>();
				list.add(sg);
				groups.add(list);
				
				GTable<String> table = new GTable<String>(sg.getQueryNodes());
				String[] row = new String[table.columnCount()];
				for (String col : table.getColumnNames()) {
					row[table.getColumn(col)] = sg.getVariableMapping().get(col);
				}
				table.addRow(row);
				indexMatches.add(table);
				
				queries.add(sg.toQuery(false));
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
