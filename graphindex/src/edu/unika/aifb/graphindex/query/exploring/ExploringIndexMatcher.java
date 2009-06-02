package edu.unika.aifb.graphindex.query.exploring;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.query.AbstractIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.StorageException;

public class ExploringIndexMatcher extends AbstractIndexGraphMatcher {

	private PriorityQueue<PriorityQueue<Cursor>> m_queues;
	private int m_maxDistance = 3; 
	
	public ExploringIndexMatcher(StructureIndex index, String graphName) {
		super(index, graphName);
		
		m_queues = new PriorityQueue<PriorityQueue<Cursor>>(20, new Comparator<PriorityQueue<Cursor>>() {
			public int compare(PriorityQueue<Cursor> o1, PriorityQueue<Cursor> o2) {
				return o1.peek().compareTo(o2.peek());
			}
		});
	}
	
	public void setKeywords(Map<String,List<GraphElement>> keywords) {
		for (String keyword : keywords.keySet()) {
			PriorityQueue<Cursor> queue = new PriorityQueue<Cursor>();
			for (GraphElement ele : keywords.get(keyword)) {
				queue.add(new Cursor(keyword, ele, null, 1));
			}
		}
	}

	@Override
	protected boolean isCompatibleWithIndex() {
		return false;
	}
	
	

	public void match() throws StorageException {
		while (m_queues.size() > 0) {
			PriorityQueue<Cursor> cursorQueue = m_queues.peek();
			Cursor minCursor = cursorQueue.poll();
			
			GraphElement currentElement = minCursor.getGraphElement();
			
			if (minCursor.getDistance() < m_maxDistance ) {
				currentElement.addCursor(minCursor);
				List<GraphElement> neighbors = currentElement.getNeighbors(minCursor.getParent() != null ? minCursor.getParent().getGraphElement() : null);
				Set<GraphElement> parents = minCursor.getParents();
				for (GraphElement neighbor : neighbors) {
					if (!parents.contains(neighbor)) {
						cursorQueue.add(new Cursor(minCursor.getKeyword(), neighbor, minCursor, minCursor.getCost() + 1));
					}
				}
			}
			
			if (cursorQueue.isEmpty())
				m_queues.remove(cursorQueue);
		}
	}

}
