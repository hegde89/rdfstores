package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class NodeCursor extends Cursor {

	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}

	@Override
	public Cursor getNextCursor(GraphElement element) {
		return new NodeCursor(m_keywords, element, this);
	}

}
