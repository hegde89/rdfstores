package edu.unika.aifb.keywordsearch.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;

public class ApproximateStructureMatcher {
	
	private int m_hops;
	
	private TransformedGraph m_graph;
	private Set<String> m_nodesWithNoEntities;
	private TransformedGraphNode m_startNode;
	private Set<KeywordElement[]> m_rows;
	private GTable<KeywordElement> m_table;
	private int m_columnSize;
	
	private static final Logger log = Logger.getLogger(ApproximateStructureMatcher.class);
	
	public ApproximateStructureMatcher(TransformedGraph graph, int hops) {
		m_graph = graph;
		m_nodesWithNoEntities = new HashSet<String>();
		m_rows = new HashSet<KeywordElement[]>();
		m_startNode = computeCentricNode();
		List<String> columnNames = new ArrayList<String>();
		columnNames.addAll(graph.getNodeNames());
		m_table = new GTable<KeywordElement>(columnNames);
		m_columnSize = columnNames.size();
		m_hops = hops;
	}
	
	public TransformedGraphNode computeCentricNode() {
		int minDistance = 0;
		int minNumOfEntities = 0;
		TransformedGraphNode centricNode = null; 
		for(TransformedGraphNode node : m_graph.getNodes()) {
			int size = node.getNumOfEntities();
			if(size != 0) {
				if(node.getMaxDistance() < minDistance || minDistance == 0) {
					minDistance = node.getMaxDistance();
					minNumOfEntities = size; 
					centricNode = node; 
				}
				else if(node.getMaxDistance() == minDistance) {
					if(size < minNumOfEntities || minNumOfEntities == 0) {
						minNumOfEntities = size;
						centricNode = node; 
					}
				}
			}
			else {
				m_nodesWithNoEntities.add(node.getNodeName());
			}
		}
		
		return centricNode;
	}
	
	public int getColumn(TransformedGraphNode node) {
		return m_table.getColumn(node.getNodeName());
	}
	
	public Collection<KeywordElement[]> getRows(KeywordElement element, int column) {
		Collection<KeywordElement[]> rows = new ArrayList<KeywordElement[]>();
		for(KeywordElement[] row : m_rows) {
			if(row[column].equals(element))
				rows.add(row);
		}
		return rows;
	}
	
	public void addRows(KeywordElement filterElement, int filterColumn, Collection<KeywordElement> elements, int elementColumn) {
		long start = System.currentTimeMillis();
		Collection<KeywordElement[]> rows = getRows(filterElement, filterColumn);
		
		if(elements.size() == 1) {
			KeywordElement element = elements.iterator().next();
			for(KeywordElement[] row : rows) {
				row[elementColumn] = element;
			}
		}
		else {
			Collection<KeywordElement[]> newRows = new ArrayList<KeywordElement[]>();
			for(KeywordElement[] row : rows) {
				for(KeywordElement element : elements) {
					KeywordElement[] newRow = (KeywordElement[])row.clone();
					newRow[elementColumn] = element;
					newRows.add(newRow);
				}
			}
			m_rows.removeAll(rows);
			m_rows.addAll(newRows);
		}
		t_add += System.currentTimeMillis() - start;
	}
	long t_rem = 0;
	long t_add = 0;
	long t_reachable = 0;
	
	public void removeRows(KeywordElement element, int column) {
		long start = System.currentTimeMillis();
		m_rows.removeAll(getRows(element, column));
		t_rem += System.currentTimeMillis() - start;
	}
	
	public void neighborhoodJoin(TransformedGraphNode filterNode, TransformedGraphNode node) {
		t_rem = t_add = t_reachable = 0;
		if(filterNode.equals(node)) {
		}
		else {
			Collection<KeywordElement> removeFilterElements = new ArrayList<KeywordElement>();
			Collection<KeywordElement> allJoinElements = new ArrayList<KeywordElement>();
			Collection<KeywordElement> elements = node.getEntities();
			
			for(KeywordElement filterElement : filterNode.getEntities()) {
				long start = System.currentTimeMillis();
				Collection<KeywordElement> joinElements = filterElement.getReachable(elements);
				t_reachable += System.currentTimeMillis() - start;
				if(joinElements == null || joinElements.size() == 0) {
					removeFilterElements.add(filterElement);
					removeRows(filterElement, getColumn(filterNode));
				}	
				else {
					allJoinElements.addAll(joinElements);
					addRows(filterElement, getColumn(filterNode), joinElements, getColumn(node));
				}	
			}
			log.debug("join/filter done");
			log.debug(elements.size() + " " + allJoinElements.size());
			elements.retainAll(allJoinElements);
			log.debug("retain done");
			if(node.getNumOfEntities() == 0)
				m_nodesWithNoEntities.add(filterNode.getNodeName());
			filterNode.removeEntities(removeFilterElements);
			log.debug("remove done");
			if(filterNode.getNumOfEntities() == 0)
				m_nodesWithNoEntities.add(filterNode.getNodeName());
			
			log.debug("t_rem: " + t_rem + ", t_add: " + t_add + ", t_reach: " + t_reachable);
		}
	}
	
	public void DFS(TransformedGraphNode node) {
		log.debug(" DFS: " + node.getNodeName());
		node.setVisisted();
		for(TransformedGraphNode neighbor : node.getNeighbors()) {
			if(neighbor.isVisited() == true 
					|| (m_startNode.getDistance(neighbor.getNodeId()) < node.getPathLength() + 1 
							&& (neighbor.getDistance(node.getFilter().getNodeId()) > m_hops 
									|| m_nodesWithNoEntities.contains(node.getFilter().getNodeName()))))
				continue;
			
			neighbor.setPathLength(node.getPathLength() + 1);
			if(neighbor.getDistance(node.getFilter().getNodeId()) > m_hops || m_nodesWithNoEntities.contains(node.getFilter().getNodeName()))  {
				if(!m_nodesWithNoEntities.contains(node.getNodeName())) {
					neighbor.setFilter(node);
				}	
				else { 
					neighbor.setFilter(neighbor);
				}	
			}
			else {
				neighbor.setFilter(node.getFilter());
			}	
			log.debug(" neighbor: " + neighbor);
			if(!m_nodesWithNoEntities.contains(neighbor.getNodeName()))
				neighborhoodJoin(neighbor.getFilter(), neighbor);
			DFS(neighbor);	
		}
	}
	
	public GTable<KeywordElement> matching() {
		if(m_startNode == null)
			return null;
		
		m_startNode.setPathLength(0);
		m_startNode.setFilter(m_startNode);
		
		int column = getColumn(m_startNode);
		for(KeywordElement ele : m_startNode.getEntities()) {
			KeywordElement[] row = new KeywordElement[m_columnSize];
			row[column] = ele;
			m_rows.add(row);
		}
		log.debug(" start node: " + m_startNode.getNodeName());
		DFS(m_startNode);
		
		List<KeywordElement[]> list = new ArrayList<KeywordElement[]>();
		list.addAll(m_rows);
		m_table.setRows(list);
		return m_table;
		
	}

}
