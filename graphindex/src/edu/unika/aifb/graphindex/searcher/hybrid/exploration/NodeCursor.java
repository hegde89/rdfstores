package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.util.Statistics;

public class NodeCursor extends Cursor {

	private static final Logger log = Logger.getLogger(NodeCursor.class);
	
	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public NodeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}

	@Override
	public Cursor getNextCursor(GraphElement element, Map<String,Set<String>> nodesWithConcepts) {
		Statistics.start(NodeCursor.class, Statistics.Timing.EX_NODECURSOR_NEXT);
		Statistics.inc(NodeCursor.class, Statistics.Counter.EX_NODECURSORS);
		if (element instanceof EdgeElement) {
			EdgeElement edge = (EdgeElement)element;
			boolean out = edge.getSource().equals(getGraphElement());
			NodeElement next = out ? edge.getTarget() : edge.getSource();
			
//			if ((out && (m_outProperties == null || m_outProperties.size() == 0 || m_outProperties.contains(edge.getLabel())))
//				|| (!out && (m_inProperties == null || m_inProperties.size() == 0 || m_inProperties.contains(edge.getLabel()))) )
			Cursor nextCursor = new NodeCursor(m_keywords, next, new EdgeCursor(m_keywords, element, this));
			Statistics.end(NodeCursor.class, Statistics.Timing.EX_NODECURSOR_NEXT);
			return nextCursor;
		}
		else
			log.error("next cursor has to be for an edge");
		Statistics.end(NodeCursor.class, Statistics.Timing.EX_NODECURSOR_NEXT);
		return null;
	}

}
