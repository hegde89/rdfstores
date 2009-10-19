package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class StructuredQueryCursor extends Cursor {

	private NodeElement m_startNode;
	private HybridQuery m_query;
	private String m_attachNode;
	
	public StructuredQueryCursor(Set<KeywordSegment> keywordSegments, GraphElement element, HybridQuery query, String attachNode) {
		super(keywordSegments, element);
		m_query = query;
		m_attachNode = attachNode;
//		m_startNode = startNode;
	}

	public StructuredQueryCursor(Set<KeywordSegment> keywordSegments, GraphElement element, Cursor parent, HybridQuery query, String attachNode) {
		super(keywordSegments, element, parent);
		m_query = query;
		m_attachNode = attachNode;
//		m_startNode = startNode;
	}

	@Override
	public Cursor getNextCursor(GraphElement element) {
		return new StructuredQueryCursor(m_keywords, element, this, m_query, m_attachNode);
	}

	public NodeElement getStartNode() {
		return m_startNode;
	}
	
	public Set<EdgeElement> getEdges() {
		Set<EdgeElement> edges = new HashSet<EdgeElement>();
		
		for (QueryEdge edge : m_query.getStructuredQuery().getQueryGraph().edgeSet()) {
			String src = edge.getSource().getLabel();
			String trg = edge.getTarget().getLabel();
			
			if (src.equals(m_attachNode))
				src = m_element.getLabel();
			
			edges.add(new EdgeElement(new NodeElement(src), edge.getLabel(), new NodeElement(trg)));
		}
		
		return edges;
	}
}
