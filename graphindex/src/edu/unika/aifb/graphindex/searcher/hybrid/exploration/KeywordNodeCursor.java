package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class KeywordNodeCursor extends NodeCursor {
	private static final Logger log = Logger.getLogger(NodeCursor.class);
	
	public KeywordNodeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public KeywordNodeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}

	@Override
	public Cursor getNextCursor(GraphElement element) {
		if (element instanceof EdgeElement) {
			EdgeElement edge = (EdgeElement)element;
			boolean out = edge.getSource().equals(getGraphElement());
			NodeElement next = out ? edge.getTarget() : edge.getSource();
			
			if ((out && m_outProperties.contains(edge.getLabel()))
				|| (!out &&  m_inProperties.contains(edge.getLabel())))
				return new NodeCursor(m_keywords, next, new EdgeCursor(m_keywords, element, this));
		}
		else
			log.error("next cursor has to be for an edge");
		return null;
	}
}
