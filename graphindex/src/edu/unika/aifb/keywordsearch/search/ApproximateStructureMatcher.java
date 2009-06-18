package edu.unika.aifb.keywordsearch.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;

public class ApproximateStructureMatcher {
	
	private int m_hops;
	
	private TransformedGraph m_graph;
	private Set<String> m_nodesWithNoEntities;
	private TransformedGraphNode m_startNode;
	private GTable<KeywordElement> m_table;
	private NeighborhoodStorage m_ns;
	
	private Timings m_timings;
	private Counters m_counters;

	
	private static final Logger log = Logger.getLogger(ApproximateStructureMatcher.class);
	
	public ApproximateStructureMatcher(TransformedGraph graph, int hops, NeighborhoodStorage ns) {
		m_graph = graph;
		m_nodesWithNoEntities = new HashSet<String>();
		m_startNode = computeCentricNode();
		m_hops = hops;
		m_ns = ns;
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
	
	long t_rem = 0;
	long t_join = 0;
	long t_reachable = 0;
	
	private KeywordElement[] combineRow(KeywordElement[] lrow, KeywordElement[] rrow, int rc) {
		KeywordElement[] resultRow = new KeywordElement[lrow.length + rrow.length - 1];
		System.arraycopy(lrow, 0, resultRow, 0, lrow.length);
		System.arraycopy(rrow, 0, resultRow, lrow.length, rc);
		System.arraycopy(rrow, rc + 1, resultRow, lrow.length + rc, rrow.length - rc - 1);
		return resultRow;
	}
	
	public GTable<KeywordElement> mergeJoin(GTable<KeywordElement> left, GTable<KeywordElement> right, String col) {
		if (!left.isSorted() || !left.getSortedColumn().equals(col) || !right.isSorted() || !right.getSortedColumn().equals(col))
			throw new UnsupportedOperationException("merge join with unsorted tables");
		long start = System.currentTimeMillis();
		if (right.columnCount() > left.columnCount()) {
			GTable<KeywordElement> temp = right;
			right = left;
			left = temp;
		}
	
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!s.equals(col))
				resultColumns.add(s);
	
		int lc = left.getColumn(col);
		int rc = right.getColumn(col);
		
		log.debug("merge join: " + left + " x " + right);
		
		GTable<KeywordElement> result = new GTable<KeywordElement>(resultColumns, left.rowCount() + right.rowCount());
	
		int l = 0, r = 0;
		while (l < left.rowCount() && r < right.rowCount()) {
			KeywordElement[] lrow = left.getRow(l);
			KeywordElement[] rrow = right.getRow(r);
	
			int val = lrow[lc].compareTo(rrow[rc]); 
			if (val < 0)
				l++;
			else if (val > 0)
				r++;
			else {
				result.addRow(combineRow(lrow, rrow, rc));
	
				KeywordElement[] row;
				int i = l + 1;
				while (i < left.rowCount() && left.getRow(i)[lc].compareTo(rrow[rc]) == 0) {
					row = left.getRow(i);
					result.addRow(combineRow(row, rrow, rc));
					i++;
				}
	
				int j = r + 1;
				while (j < right.rowCount() && lrow[lc].compareTo(right.getRow(j)[rc]) == 0) {
					row = right.getRow(j);
					result.addRow(combineRow(lrow, row, rc));
					j++;
				}
	
				l++;
				r++;
			}
		}
		
		result.setSortedColumn(lc);
		t_join += System.currentTimeMillis() - start;
		return result;
	}
	
	public void removeRows(Set<KeywordElement> elements, int column) {
		long start = System.currentTimeMillis();
		Iterator<KeywordElement[]> iter = m_table.iterator();
		while(iter.hasNext()) {
			if(elements.contains(iter.next()[column]))
				iter.remove();
		}
		
		t_rem += System.currentTimeMillis() - start;
	}
	
	public void neighborhoodJoin(TransformedGraphNode filterNode, TransformedGraphNode node) {
		t_rem = t_join = t_reachable = 0;
		if(filterNode.equals(node)) {
		}
		else {
			Collection<KeywordElement> filterElements = filterNode.getEntities();
			Collection<KeywordElement> elements = node.getEntities();
			List<KeywordElement> remainingFilterElements = new ArrayList<KeywordElement>(filterElements.size());
			Set<KeywordElement> removedFilterElements = new HashSet<KeywordElement>(filterElements.size());
			Set<KeywordElement> allJoinedElements = new HashSet<KeywordElement>(filterElements.size());
			GTable<KeywordElement> joinedTable = new GTable<KeywordElement>(filterNode.getNodeName(), node.getNodeName());
			
			for(KeywordElement filterElement : filterElements) {
				long start = System.currentTimeMillis();
				m_timings.start(Timings.ASM_REACHABLE);
				Collection<KeywordElement> joinedElements = filterElement.getReachable(elements);
				m_timings.end(Timings.ASM_REACHABLE);
				t_reachable += System.currentTimeMillis() - start;
				if(joinedElements == null || joinedElements.size() == 0) {
					removedFilterElements.add(filterElement);
				}	
				else {
					remainingFilterElements.add(filterElement);
					allJoinedElements.addAll(joinedElements);
					for(KeywordElement ele : joinedElements) {
						KeywordElement[] row = new KeywordElement[2];
						row[0] = filterElement;
						row[1] = ele;
						joinedTable.addRow(row);
					}
				}	
			}
			
			removeRows(removedFilterElements, getColumn(filterNode));
			m_table.sort(filterNode.getNodeName(), true);
			joinedTable.sort(filterNode.getNodeName());
			m_table = mergeJoin(m_table, joinedTable, filterNode.getNodeName());
			
			log.debug("join/filter done");
			log.debug(filterElements.size() + " " + remainingFilterElements.size());
			log.debug(elements.size() + " " + allJoinedElements.size());
			
			node.setEntities(allJoinedElements);
			
			if(node.getNumOfEntities() == 0)
				m_nodesWithNoEntities.add(filterNode.getNodeName());
			filterNode.setEntities(remainingFilterElements);
			
			if(filterNode.getNumOfEntities() == 0)
				m_nodesWithNoEntities.add(filterNode.getNodeName());
			
			log.debug("t_rem: " + t_rem + ", t_join: " + t_join + ", t_reach: " + t_reachable);
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
		
		m_table = new GTable<KeywordElement>(m_startNode.getNodeName());
		List<KeywordElement[]> list = new ArrayList<KeywordElement[]>();
		for(KeywordElement ele : m_startNode.getEntities()) {
			KeywordElement[] row = new KeywordElement[1];
			row[0] = ele;
			list.add(row);
		}
		m_table.setRows(list);
		m_table.sort(m_startNode.getNodeName());
		
		log.debug(" start node: " + m_startNode.getNodeName());
		DFS(m_startNode);
		
		return m_table;
		
	}

	public void setCounters(Counters counters) {
		m_counters = counters;
	}

	public void setTimings(Timings timings) {
		m_timings = timings;
	}
	
	


}
