package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StatisticsCollector;

public class QueryMappingListener implements MappingListener {
	private StructureIndex m_index;
	private Graph<QueryNode> m_origQueryGraph;
	private StatisticsCollector m_collector;
	private ExecutorCompletionService<List<String[]>> m_completionService;
	private Set<String> m_noIncoming;
	private List<Map<String,String>> m_mappings;
	private StructureIndexReader m_indexReader;
	private Set<String> m_signatures;
	
	private static final Logger log = Logger.getLogger(QueryMappingListener.class);

	public QueryMappingListener(Graph<QueryNode> orig, Graph<String> indexGraph, StructureIndexReader indexReader, VCompatibilityCache vcc, ExecutorCompletionService<List<String[]>> completionService) {
		m_indexReader = indexReader;
		m_index = m_indexReader.getIndex();
		
		m_origQueryGraph = orig;
		m_completionService = completionService;
		m_collector = m_index.getCollector();
		m_mappings = new ArrayList<Map<String,String>>();
		m_signatures = new HashSet<String>();
		
		m_noIncoming = new HashSet<String>();
		for (int i = 0; i < m_origQueryGraph.nodeCount(); i++) {
			QueryNode qn = m_origQueryGraph.getNode(i);
		
			if (m_origQueryGraph.inDegreeOf(i) == 0)
				for (String member : qn.getMembers())
					m_noIncoming.add(member);
			
		}
		
	}
	
	private String getSignature(Map<String,String> map) {
		String sig = "";
		for (int i = 0; i < m_origQueryGraph.nodeCount(); i++) {
			String k = m_origQueryGraph.getNode(i).getSingleMember();
			if (m_noIncoming.contains(k))
				continue;
			sig += map.get(k) + "__";
		}
		return sig;
	}

	public void mapping(VertexMapping mapping) {
		Map<String,String> map = new HashMap<String,String>();
		for (int node = 0; node < m_origQueryGraph.nodeCount(); node++) {
			QueryNode qn = m_origQueryGraph.getNode(node);
//			if (!m_vcc.get(qn.getSingleMember(), mapping.getVertexCorrespondence(qn.getName(), true)))
//				return;
			map.put(qn.getSingleMember(), mapping.getVertexCorrespondence(qn.getName(), true));
		}
		
		String sig = getSignature(map);
		
		if (m_signatures.contains(sig))
			return;
			
		m_mappings.add(map);
		m_signatures.add(sig);
	}

	public List<Map<String,String>> generateMappings() throws StorageException {
		log.debug("mappings: " + m_mappings.size());

		return m_mappings;
	}
}
