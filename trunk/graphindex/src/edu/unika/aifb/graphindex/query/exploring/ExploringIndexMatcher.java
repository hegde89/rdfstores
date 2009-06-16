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
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.keywordsearch.KeywordSegement;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 10; 
	private DirectedMultigraph<NodeElement,EdgeElement> m_indexGraph;
	private Map<String,NodeElement> m_nodes;
	private Map<KeywordSegement,List<GraphElement>> m_keywordSegments;
	private Set<String> m_keywords;
	private List<Subgraph> m_subgraphs;
	private int m_k = 10;
	private Map<KeywordSegement,PriorityQueue<Cursor>> m_keywordQueues;
	private Map<String,Set<KeywordSegement>> m_edgeUri2Keywords; 
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
		m_keywordQueues = new HashMap<KeywordSegement,PriorityQueue<Cursor>>();
		m_edgeUri2Keywords = new HashMap<String,Set<KeywordSegement>>();
		m_edgesWithCursors = new HashSet<EdgeElement>();
		m_keywords = new HashSet<String>();
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
	
	public void setKeywords(Map<KeywordSegement,List<GraphElement>> keywords) {
//		log.debug(keywords);
		for (KeywordSegement keyword : keywords.keySet()) {
			PriorityQueue<Cursor> queue = new PriorityQueue<Cursor>();
			m_keywords.addAll(keyword.getKeywords());
			for (GraphElement ele : keywords.get(keyword)) {
				if (ele instanceof NodeElement) {
					NodeElement node = m_nodes.get(ele.getLabel());
					queue.add(new Cursor(keyword, node, null, 0));
				}
				else if (ele instanceof EdgeElement) {
					Set<KeywordSegement> edgeKeywords = m_edgeUri2Keywords.get(ele.getLabel());
					if (edgeKeywords == null) {
						edgeKeywords = new HashSet<KeywordSegement>();
						m_edgeUri2Keywords.put(ele.getLabel(), edgeKeywords);
					}
					edgeKeywords.add(keyword);
				}
			}
			log.debug("queue size for " + keyword + ": " + queue.size());
			if (!queue.isEmpty()) {
				m_queues.add(queue);
				m_keywordQueues.put(keyword, queue);
			}
		}
		log.debug("keywords: " + m_keywords);

		m_keywordSegments = keywords;
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
				Set<String> combinationKeywords = new HashSet<String>();
				for (Cursor c : combination)
					for (KeywordSegement ks : c.getKeywordSegments())
						combinationKeywords.addAll(ks.getKeywords());
				
				if (!combinationKeywords.equals(m_keywords)) {
					log.debug(combination);
					continue;
				}
				Subgraph sg = new Subgraph(new HashSet<Cursor>(combination));

				if (!m_subgraphs.contains(sg)) {
					if (!sg.hasDanglingEdge())
						m_subgraphs.add(sg);
				}
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
								for (KeywordSegement ks : m_edgeUri2Keywords.get(neighbor.getLabel()))
									c.addKeywordSegment(ks);
								cursorQueue.add(c);
							}
							else
								cursorQueue.add(new Cursor(minCursor.getKeywordSegments(), neighbor, minCursor, minCursor.getCost() + 1));
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
	
	public void indexMatches(List<GTable<String>> indexMatches, List<Query> queries, List<Map<String,Set<KeywordSegement>>> select2ks, boolean withAttributes) {
		List<List<Subgraph>> groups = new ArrayList<List<Subgraph>>();
		
		for (Subgraph sg : m_subgraphs) {
			boolean found = false;
			for (int i = 0; i < groups.size(); i++) {
				List<Subgraph> list = groups.get(i);
				List<Map<String,String>> mappings = list.get(0).isIsomorphicTo(sg);
				if (mappings.size() > 0) {
					list.add(sg);
					
					Map<String,String> vars = list.get(0).getVariableMapping();
					GTable<String> table = indexMatches.get(i);
					
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
//				log.debug(sg.edgeSet());
//				log.debug("");
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
				
//				queries.add(sg.toQuery(withAttributes));
				
				Map<String,Set<KeywordSegement>> ksMap = sg.getKSMapping();
				Map<KeywordSegement,KeywordSegement> replace = new HashMap<KeywordSegement,KeywordSegement>();
				
				Set<KeywordSegement> querySegments = new HashSet<KeywordSegement>();
				for (Set<KeywordSegement> ksSet : ksMap.values())
					for (KeywordSegement ks : ksSet)
						querySegments.add(ks);
				
				Set<String> compared = new HashSet<String>();
				for (KeywordSegement ks1 : querySegments) {
					for (KeywordSegement ks2 : querySegments) {
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
							for (KeywordSegement ks : m_keywordSegments.keySet()) {
								if (ks.getKeywords().equals(remaining)) {
									replace.put(ks1, ks);
									break;
								}
							}
						}
					}
				}
//				log.debug(replace);
				for (Set<KeywordSegement> ksSet : ksMap.values()) {
					for (KeywordSegement ks : replace.keySet())
						if (ksSet.contains(ks)) {
							ksSet.remove(ks);
							ksSet.add(replace.get(ks));
						}
							
				}
//				log.debug(ksMap);
				select2ks.add(ksMap);
				
				Query q = new Query(null);
				q.setSelectVariables(sg.getSelectVariables());
				
				for (EdgeElement e : sg.edgeSet()) {
					String src = sg.getLabels().get(e.getSource().getLabel());
					String dst = sg.getLabels().get(e.getTarget().getLabel());
					q.addLiteral(new Literal(new Predicate(e.getLabel()), new Variable(src), new Variable(dst)));
				}
				
				if (withAttributes) {
					int x = 0;
					for (String selectNode : ksMap.keySet()) {
						for (KeywordSegement ks : ksMap.get(selectNode))
							for (String keyword : ks.getKeywords())
								q.addLiteral(new Literal(new Predicate("???" + x++), new Variable(selectNode), new Constant(keyword)));
					}
				}
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
