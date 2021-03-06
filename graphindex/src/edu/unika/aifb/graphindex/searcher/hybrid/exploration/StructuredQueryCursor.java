package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;

public class StructuredQueryCursor extends NodeCursor {

	private NodeElement m_startNode;
	private HybridQuery m_query;
	private String m_attachNode;
	public Set<String> entities;
	
	private static final Logger log = Logger.getLogger(StructuredQueryCursor.class);

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
	public boolean acceptsEdge(EdgeElement edge) {
		boolean out = edge.getSource().equals(getGraphElement());
		
		if (out)
			return m_outProperties.contains(edge.getLabel());
		else
			return m_inProperties.contains(edge.getLabel());
	}

	public Cursor getNextCursor(GraphElement element, Map<String,Set<String>> nodesWithConcepts) {
//		return new StructuredQueryCursor(m_keywords, element, this, m_query, m_attachNode);
		if (element instanceof EdgeElement) {
			EdgeElement edge = (EdgeElement)element;
			boolean out = edge.getSource().equals(getGraphElement());
			NodeElement next = out ? edge.getTarget() : edge.getSource();
			
//			if ((out && (m_outProperties == null || m_outProperties.size() == 0 || m_outProperties.contains(edge.getLabel())))
//				|| (!out && (m_inProperties == null || m_inProperties.size() == 0 || m_inProperties.contains(edge.getLabel()))) )
			if ((out && m_outProperties.contains(edge.getLabel())) || (!out &&  m_inProperties.contains(edge.getLabel()))) {
				Cursor nextEdge = new EdgeCursor(m_keywords, element, this);
				nextEdge.setCost(getCost() + 1 - (out ? m_outPropertyWeights.get(edge.getLabel()) : m_inPropertyWeights.get(edge.getLabel())));
				Cursor nextCursor = new NodeCursor(m_keywords, next, nextEdge);

				return nextCursor;
//				return new NodeCursor(m_keywords, next, new EdgeCursor(m_keywords, element, this));
			}
		}
		else
			log.error("next cursor has to be for an edge");
		return null;
	}

	public NodeElement getStartNode() {
		return m_startNode;
	}
	
	public HybridQuery getQuery() {
		return m_query;
	}
	
	@Override
	public List<GraphElement> getPath() {
		List<GraphElement> path = new ArrayList<GraphElement>();

		if (m_query.getStructuredQuery() != null) {
			for (QueryEdge edge : m_query.getStructuredQuery().getQueryGraph().edgeSet()) {
				String src = edge.getSource().getLabel();
				String trg = edge.getTarget().getLabel();
				
				if (src.equals(m_attachNode))
					src = m_element.getLabel();
				
				path.add(new EdgeElement(new NodeElement(src), edge.getLabel(), new NodeElement(trg)));
			}
		}
		else {
//			path.add(new NodeElement(m_element.getLabel()));
		}
		
		return path;
	}
	
	@Override
	public Set<EdgeElement> getEdges() {
		Set<EdgeElement> edges = new HashSet<EdgeElement>();
		
		if (m_query.getStructuredQuery() != null) {
			for (QueryEdge edge : m_query.getStructuredQuery().getQueryGraph().edgeSet()) {
				String src = edge.getSource().getLabel();
				String trg = edge.getTarget().getLabel();
				
				if (src.equals(m_attachNode))
					src = m_element.getLabel();
				
				edges.add(new EdgeElement(new NodeElement(src), edge.getLabel(), new NodeElement(trg)));
			}
		}
		
		return edges;
	}
}
