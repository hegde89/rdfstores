package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.query.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.StorageException;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 5; 
	private DirectedMultigraph<NodeElement,EdgeElement> m_indexGraph;
	private Map<String,NodeElement> m_nodes;
	private Map<String,List<GraphElement>> m_keywords;
	private List<Subgraph> m_subgraphs;
	private int m_k = 20;
	
	private static final Logger log = Logger.getLogger(ExploringIndexMatcher.class);
	
	public ExploringIndexMatcher(StructureIndex index, String graphName) {
		super(index, graphName);
	
		m_subgraphs = new ArrayList<Subgraph>();
		m_nodes = new HashMap<String,NodeElement>();
		m_queues = new PriorityQueue<PriorityQueue<Cursor>>(20, new Comparator<PriorityQueue<Cursor>>() {
			public int compare(PriorityQueue<Cursor> o1, PriorityQueue<Cursor> o2) {
				return o1.peek().compareTo(o2.peek());
			}
		});
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
				queue.add(new Cursor(keyword, m_nodes.get(ele.getLabel()), null, 0));
			}
			m_queues.add(queue);
		}
		m_keywords = keywords;
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

				if (!m_subgraphs.contains(sg)) {
					m_subgraphs.add(sg);
					
//					log.debug("connecting: " + currentElement + ", " + currentElement.getKeywords().size());
//					for (String keyword : currentElement.getKeywords().keySet()) {
//						log.debug("keyword: " + keyword);
//						for (Cursor c : currentElement.getKeywords().get(keyword))
//							log.debug(" " + c + " " + c.getPath().size());
//					}
//					log.debug(m_subgraphs.size());
				}
			}
		}
		
		if (m_subgraphs.size() < m_k)
			return false;

		Collections.sort(m_subgraphs);
		
//		int i = 0;
//		Subgraph last = null;
//		for (Subgraph sg : m_subgraphs) {
//			if (i >= m_k) {
//				last = sg;
//				break;
//			}
//			i++;
//		}

		for (int i = m_subgraphs.size() - 1; i > m_k; i--)
			m_subgraphs.remove(i);

//		if (last != null)
//			m_subgraphs = m_subgraphs.headSet(last);
		
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
		while (m_queues.size() > 0) {
			PriorityQueue<Cursor> cursorQueue = m_queues.poll();
			Cursor minCursor = cursorQueue.poll();
			GraphElement currentElement = minCursor.getGraphElement();
			
			if (minCursor.getDistance() < m_maxDistance) {
//				log.debug("min cursor: " + minCursor);
				currentElement.addCursor(minCursor);

				if (minCursor.getDistance() < m_maxDistance - 1) {
					List<GraphElement> neighbors = currentElement.getNeighbors(m_indexGraph, minCursor);
					Set<GraphElement> parents = minCursor.getParents();
					for (GraphElement neighbor : neighbors) {
						if (!parents.contains(neighbor))
							cursorQueue.add(new Cursor(minCursor.getKeyword(), neighbor, minCursor, minCursor.getCost() + 1));
					}
				}
				
				boolean done = topK(currentElement);
				
				if (done)
					break;

//				if (currentElement.getKeywords().size() == m_keywords.size()) {
//					log.debug(currentElement + ", " + currentElement.getKeywords().size());
//					log.debug(minCursor.getPath() + " " + currentElement.getKeywords());
//					
//					for (String keyword : currentElement.getKeywords().keySet()) {
//						log.debug("keyword: " + keyword);
//						for (Cursor c : currentElement.getKeywords().get(keyword))
//							log.debug(" " + c + " " + c.getPath());
//					}
//				}
			}
			
			if (!cursorQueue.isEmpty())
				m_queues.add(cursorQueue);
		}
		
		for (Subgraph sg : m_subgraphs) {
			log.debug(sg);
			sg.toQuery();
		}
	}
}
