package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Statistics;

public class KeywordNodeCursor extends NodeCursor {
	public KeywordElement m_keywordElement;

	private DataIndex m_dataIndex;
	
	private Map<String,Set<String>> propertyConcepts = new HashMap<String,Set<String>>();
	
	private static final Logger log = Logger.getLogger(KeywordNodeCursor.class);
	
	public KeywordNodeCursor(Set<KeywordSegment> keywords, GraphElement element) {
		super(keywords, element);
	}

	public KeywordNodeCursor(Set<KeywordSegment> keywords, GraphElement element, Cursor parent) {
		super(keywords, element, parent);
	}
	
	public void setDataIndex(DataIndex dataIndex) {
		m_dataIndex = dataIndex;
	}

	@Override
	public boolean acceptsEdge(EdgeElement edge) {
		boolean out = edge.getSource().equals(getGraphElement());
		
		if (out)
			return m_outProperties.contains(edge.getLabel());
		else
			return m_inProperties.contains(edge.getLabel());
	}

	@Override
	public Cursor getNextCursor(GraphElement element, Map<String,Set<String>> nodesWithConcepts) {
		Statistics.start(KeywordNodeCursor.class, Statistics.Timing.EX_KWCURSOR_NEXT);
		if (element instanceof EdgeElement) {
			EdgeElement edge = (EdgeElement)element;
			boolean out = edge.getSource().equals(getGraphElement());
			NodeElement next = out ? edge.getTarget() : edge.getSource();
			
			if ((out && m_outProperties.contains(edge.getLabel())) || (!out &&  m_inProperties.contains(edge.getLabel()))) {
				boolean found = false;
				try {
					String nextNode = out ? edge.getTarget().getLabel() : edge.getSource().getLabel();
					if (nodesWithConcepts.containsKey(nextNode)) {
						Set<String> concepts = nodesWithConcepts.get(nextNode);
						Set<String> nextConcepts = new HashSet<String>();
						
						if (propertyConcepts.containsKey(edge.getLabel())) {
							for (String nextConcept : propertyConcepts.get(edge.getLabel())) {
								if (concepts.contains(nextConcept)) {
									found = true;
									break;
								}
							}
						}
						else {
							for (String localEntity : m_keywordElement.entities) {
								IndexDescription idx = m_dataIndex.getSuitableIndex(DataField.PROPERTY, out ? DataField.SUBJECT : DataField.OBJECT);
								List<String> nextEntities = m_dataIndex.getIndexStorage(idx).getDataList(idx, 
									out ? DataField.OBJECT : DataField.SUBJECT, 
									idx.createValueArray(DataField.PROPERTY, edge.getLabel(), out ? DataField.SUBJECT : DataField.OBJECT, localEntity));
								
								idx = m_dataIndex.getSuitableIndex(DataField.PROPERTY, DataField.SUBJECT);
								for (String nextEntity : nextEntities) {
									List<String> nextEntityConcepts = m_dataIndex.getIndexStorage(idx).getDataList(idx,
										DataField.OBJECT, 
										idx.createValueArray(DataField.PROPERTY, RDF.TYPE.toString(), DataField.SUBJECT, nextEntity));
									
									nextConcepts.addAll(nextEntityConcepts);
									for (String nextEntityConcept : nextEntityConcepts) {
										if (concepts.contains(nextEntityConcept)) {
											found = true;
											break;
										}
									}
									
									if (found)
										break;
								}
								
								if (found)
									break;
							}
							
							if (!found) {
								propertyConcepts.put(edge.getLabel(), nextConcepts);
//								log.debug(this + " " + edge.getLabel() +  " " + nextNode + " faild");
							}
						}
					}
					else
						found = true;
				} catch (StorageException e) {
					e.printStackTrace();
				}
//				if (found)
//					log.debug(this + " " + edge.getLabel());
				
				if (!found) {
					return null;
				}
				
				
				Cursor nextEdge = new EdgeCursor(m_keywords, element, this);
				nextEdge.setCost(getCost() + (out ? m_outPropertyWeights.get(edge.getLabel()) : m_inPropertyWeights.get(edge.getLabel())));
				Cursor nextCursor = new NodeCursor(m_keywords, next, nextEdge);

				if (track && edge.getLabel().contains("writer"))
					log.debug(this + " => " + edge + " " + nextCursor);
				
				Statistics.end(KeywordNodeCursor.class, Statistics.Timing.EX_KWCURSOR_NEXT);
				return nextCursor;
			}

		}
		else
			log.error("next cursor has to be for an edge");
		Statistics.end(KeywordNodeCursor.class, Statistics.Timing.EX_KWCURSOR_NEXT);
		return null;
	}
}
