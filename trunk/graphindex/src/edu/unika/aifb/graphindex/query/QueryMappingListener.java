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
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryMappingListener implements MappingListener {
	private StructureIndex m_index;
	private Query m_query;
	private Graph<QueryNode> m_origQueryGraph;
	private StatisticsCollector m_collector;
	private ExecutorCompletionService<List<String[]>> m_completionService;
	private List<String> m_signatureNodes;
	private List<Map<String,String>> m_mappings;
	private StructureIndexReader m_indexReader;
	private Set<String> m_signatures;
	private Timings t;
	
	private static final Logger log = Logger.getLogger(QueryMappingListener.class);

	public QueryMappingListener(StructureIndexReader indexReader) {
		m_indexReader = indexReader;
		m_index = m_indexReader.getIndex();
//		m_completionService = completionService;
		m_collector = m_index.getCollector();
		t = new Timings();
		m_index.getCollector().addTimings(t);
	}
	
	public void setQueryGraph(Query query, Graph<QueryNode> queryGraph) {
		m_query = query;
		m_origQueryGraph = queryGraph;
		
		m_mappings = new ArrayList<Map<String,String>>();
		m_signatures = new HashSet<String>();
		m_signatureNodes = new ArrayList<String>();
		
		for (int i = 0; i < m_origQueryGraph.nodeCount(); i++) {
			if (m_origQueryGraph.inDegreeOf(i) > 0)
				m_signatureNodes.add(m_origQueryGraph.getNode(i).getSingleMember());
		}
		
		if (m_query.getRemovedNodes().size() > 0) {
			for (String node : m_query.getRemovedNodes()) {
				if (!m_signatureNodes.contains(node))
					m_signatureNodes.remove(node);
			}
			log.debug("sig nodes: " + m_signatureNodes);
		}
	}
	
	private String getSignature(Map<String,String> map) {
		StringBuilder sig = new StringBuilder();
		for (String k : m_signatureNodes)
			sig.append(map.get(k)).append("__");
		return sig.toString();
	}

	public void mapping(Map<String,String> map) {
		t.start(Timings.ML);
		String sig = getSignature(map);
		
		if (m_signatures.contains(sig)) {
			t.end(Timings.ML);
			return;
		}
			
		m_mappings.add(map);
		m_signatures.add(sig);
		t.end(Timings.ML);
	}

	public List<Map<String,String>> generateMappings() throws StorageException {
		log.debug("mappings: " + m_mappings.size());

		if (m_query.getRemovedNodes().size() > 0) {
			for (Map<String,String> map : m_mappings) {
//				log.debug(map);
				for (String node : m_query.getRemovedNodes())
					map.remove(node);
			}
//			log.debug("removed nodes");
		}
		
		return m_mappings;
	}
}
