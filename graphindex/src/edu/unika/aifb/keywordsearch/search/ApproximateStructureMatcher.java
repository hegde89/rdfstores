package edu.unika.aifb.keywordsearch.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;

public class ApproximateStructureMatcher {
	
	private int m_hops;
	
	private TransformedGraph m_graph;
	private Set<String> m_nodesWithNoEntities;
	private TransformedGraphNode m_startNode;
	private Map<KeywordElement, Collection<KeywordElement[]>> m_ele2rows;
	private GTable<KeywordElement> m_table;
	private int m_columnSize;
	
	
	public ApproximateStructureMatcher(TransformedGraph graph, int hops) {
		m_graph = graph;
		m_nodesWithNoEntities = new HashSet<String>();
		m_ele2rows = new HashMap<KeywordElement, Collection<KeywordElement[]>>();
		m_startNode = computeCentricNode();
		m_startNode.setPathLength(0);
		m_startNode.setFilter(m_startNode);
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
	
	public void addToRows(KeywordElement filterElement, Collection<KeywordElement> elements, int column) {
		Collection<KeywordElement[]> rows = m_ele2rows.get(filterElement);
		Collection<KeywordElement[]> newRows = new ArrayList<KeywordElement[]>();
		for(KeywordElement[] row : rows) {
			for(KeywordElement element : elements) {
				KeywordElement[] newRow = (KeywordElement[])row.clone();
				newRow[column] = element;
				newRows.add(newRow);
			}
		}
		m_ele2rows.remove(filterElement);
		m_ele2rows.put(filterElement, newRows);
		for(KeywordElement element : elements) {
			m_ele2rows.put(element, newRows);
		}
	}
	
	public void removeRows(KeywordElement element) {
		m_ele2rows.remove(element);
	}
	
	public void neighborhoodJoin(TransformedGraphNode filterNode, TransformedGraphNode node) {
		if(filterNode.equals(node)) {
		}
		else {
			Collection<KeywordElement> elementsToRemove = new ArrayList<KeywordElement>();
			Collection<KeywordElement> elements = node.getEntities();
			for(KeywordElement filterElement : filterNode.getEntities()) {
				Collection<KeywordElement> joinElements = filterElement.getReachable(elements);
				if(joinElements == null || joinElements.size() == 0)
					elementsToRemove.add(filterElement);
				else
					addToRows(filterElement, joinElements, getColumn(node));
			}
			for(KeywordElement elementToRemove : elementsToRemove)
				removeRows(elementToRemove);
		}
	}
	
	public void DFS(TransformedGraphNode node) {
		node.setVisisted();
		for(TransformedGraphNode neighbor : node.getNeighbors()) {
			if(neighbor.isVisited() == true 
					|| (m_startNode.getDistance(neighbor.getNodeId()) < node.getPathLength() + 1 
							&& neighbor.getDistance(node.getFilter().getNodeId()) > m_hops))
				continue;
			neighbor.setPathLength(node.getPathLength() + 1);
			if(neighbor.getDistance(node.getFilter().getNodeId()) > m_hops)  {
				if(!m_nodesWithNoEntities.contains(node))
					neighbor.setFilter(node);
				else 
					neighbor.setFilter(neighbor);
			}
			else 
				neighbor.setFilter(node.getFilter());
			if(!m_nodesWithNoEntities.contains(neighbor))
				neighborhoodJoin(neighbor.getFilter(), neighbor);
			DFS(neighbor);	
		}
	}
	
	public GTable<KeywordElement> matching() {
		if(m_startNode == null)
			return null;
		
		int column = getColumn(m_startNode);
		for(KeywordElement ele : m_startNode.getEntities()) {
			KeywordElement[] row = new KeywordElement[m_columnSize];
			row[column] = ele;
			Collection<KeywordElement[]> coll = m_ele2rows.get(ele);
			if(coll == null) {
				coll = new ArrayList<KeywordElement[]>();
				m_ele2rows.put(ele, coll);
			}
			coll.add(row);
		}
		
		DFS(m_startNode);
		
		List<KeywordElement[]> list = new ArrayList<KeywordElement[]>();
		for(Collection<KeywordElement[]> coll : m_ele2rows.values()) {
			list.addAll(coll);
		}
		m_table.setRows(list);
		return m_table;
		
	}

}
