package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class NodeCursor extends Cursor {

	private static final Logger log = Logger.getLogger(NodeCursor.class);
	
	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}

	@Override
	public Cursor getNextCursor(GraphElement element) {
		if (element instanceof EdgeElement) {
			EdgeElement edge = (EdgeElement)element;
			NodeElement next = edge.getSource().equals(getGraphElement()) ? edge.getTarget() : edge.getSource();
			return new NodeCursor(m_keywords, next, new EdgeCursor(m_keywords, element, this));
		}
		else
			log.error("next cursor has to be for edge");
		return null;
	}

}
