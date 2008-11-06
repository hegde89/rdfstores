package edu.unika.aifb.graphindex.query;

import java.util.HashMap;
import java.util.Map;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public class VCompatibilityCache {
	private class Pair {
		public int n1, n2;

		public Pair(int n1, int n2) {
			this.n1 = n1;
			this.n2 = n2;
		}

		@Override
		public int hashCode() {
			return n1 * 31 + n2;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Pair other = (Pair)obj;
			if (n1 != other.n1)
				return false;
			if (n2 != other.n2)
				return false;
			return true;
		}
	}
	
	private Map<Pair,Boolean> m_cache;
	private Map<String,Boolean> m_stringCache;
	private ExtensionStorage m_es;
	private Graph<QueryNode> m_orig;
	private Timings m_timings;
	private StructureIndex m_index;
	private Graph<String> m_curIndexGraph;
	
	public VCompatibilityCache(Graph<QueryNode> orig, StructureIndex index) {
		m_cache = new HashMap<Pair,Boolean>();
		m_stringCache = new HashMap<String,Boolean>();
		m_index = index;
		m_es = index.getExtensionManager().getExtensionStorage();
		m_orig = orig;
		m_timings = new Timings();
		m_index.getCollector().addTimings(m_timings);
	}
	
	private String getCacheString(String groundTerm, String ext) {
		return new StringBuilder().append(groundTerm).append("__").append(ext).toString();
	}
	
	private Pair getPair(int n1, int n2) {
		return new Pair(n1, n2);
	}
	
	public void put(int n1, int n2, boolean value) {
		m_cache.put(getPair(n1, n2), value);
	}
	
	public Boolean get(int n1, int n2) {
		return m_cache.get(getPair(n1, n2));
	}
	
	public boolean get(String v1, String v2) {
		Boolean value = m_stringCache.get(getCacheString(v1, v2));
		if (value != null)
			return value.booleanValue();
		
		if (v1.startsWith("?")) {
			m_stringCache.put(getCacheString(v1, v2), true);
			return true;
		}
		
		m_timings.start(Timings.GT);

		int node1 = m_orig.getNodeId(new QueryNode(v1));
		
		value = true;
		if (m_orig.inDegreeOf(node1) > 0) {
			for (String label : m_orig.inEdgeLabels(node1)) {
				try {
					if (!m_es.hasTriples(v2, label, v1)) {
						value = false;
						break;
					}
					else {
						value = true;
						break;
					}
				} catch (StorageException e) {
					e.printStackTrace();
				}
			}
		}
		else {
			int node2 = m_curIndexGraph.getNodeId(v2);
			if (m_curIndexGraph.inDegreeOf(node2) > 0) {
				for (String label : m_curIndexGraph.inEdgeLabels(node2)) {
					try {
						if (!m_es.hasTriples(v2, label, v1)) {
							value = false;
							break;
						}
						else {
							value = true;
							break;
						}
					} catch (StorageException e) {
						e.printStackTrace();
					}
				}
			}
		}
		m_stringCache.put(getCacheString(v1, v2), value);
		
		m_timings.end(Timings.GT);
		
		return value.booleanValue();
	}

	public void clear() {
		m_cache = new HashMap<Pair,Boolean>();
	}

	public int size() {
		return m_stringCache.size();
	}

	public void setCurrentIndexGraph(Graph<String> indexGraph) {
		m_curIndexGraph = indexGraph;
	}
}
