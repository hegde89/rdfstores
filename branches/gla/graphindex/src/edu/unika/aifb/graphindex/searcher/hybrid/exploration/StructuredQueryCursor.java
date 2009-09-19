package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class StructuredQueryCursor extends Cursor {

	private NodeElement m_startNode;
	
	public StructuredQueryCursor(Set<KeywordSegment> keywordSegments, GraphElement element) {
		super(keywordSegments, element);
//		m_startNode = startNode;
	}

	public StructuredQueryCursor(Set<KeywordSegment> keywordSegments, GraphElement element, Cursor parent) {
		super(keywordSegments, element, parent);
//		m_startNode = startNode;
	}

	@Override
	public Cursor getNextCursor(GraphElement element) {
		return new StructuredQueryCursor(m_keywords, element, this);
	}

	public NodeElement getStartNode() {
		return m_startNode;
	}
	
//	public Set<EdgeElement> getEdges() {
//		Set<EdgeElement> edges = new HashSet<EdgeElement>();
//		edges.addAll(super.getEdges());
//		edges.addAll(((StructuredMatchElement)getStartElement()).getQueryEdges());
//		return edges;
//	}

}
