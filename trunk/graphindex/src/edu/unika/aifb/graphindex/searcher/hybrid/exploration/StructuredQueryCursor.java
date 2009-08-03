package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.exploration.Cursor;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.GraphElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.NodeElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class StructuredQueryCursor extends Cursor {

	private NodeElement m_startNode;
	
	public StructuredQueryCursor(KeywordSegment keyword, GraphElement element, Cursor parent, int cost, NodeElement startNode) {
		super(keyword, element, parent, cost);
		m_startNode = startNode;
	}

	public StructuredQueryCursor(Set<KeywordSegment> keywordSegments, GraphElement neighbor, Cursor minCursor, int cost, NodeElement startNode) {
		super(keywordSegments, neighbor, minCursor, cost);
		m_startNode = startNode;
	}

	public NodeElement getStartNode() {
		return m_startNode;
	}
	
	public Set<EdgeElement> getEdges() {
		Set<EdgeElement> edges = new HashSet<EdgeElement>();
		edges.addAll(super.getEdges());
		edges.addAll(((StructuredMatchElement)getStartElement()).getQueryEdges());
		return edges;
	}
}
