package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class EdgeCursor extends Cursor {

	public EdgeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public EdgeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}

	@Override
	public Cursor getNextCursor(GraphElement element, Map<String,Set<String>> nodesWithConcepts) {
		return new NodeCursor(m_keywords, element, this);
	}

}
