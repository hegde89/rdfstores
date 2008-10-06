package edu.unika.aifb.graphindex;

import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;

public class QueryFeasibilityChecker implements FeasibilityChecker {

	private VCompatibilityCache m_vcc;
	private Timings m_timings;
	private LuceneExtensionStorage m_les;
	private IndexGraph m_qg, m_ig;

	public QueryFeasibilityChecker(IndexGraph queryGraph, IndexGraph indexGraph, VCompatibilityCache cache, Timings t, LuceneExtensionStorage les) {
		m_qg = queryGraph;
		m_ig = indexGraph;
		m_vcc = cache;
		m_timings = t;
		m_les = les;
	}
	
	public boolean isEdgeCompatible(IndexEdge e1, IndexEdge e2) {
		return e1.getLabel().equals(e2.getLabel());
	}

	public boolean isVertexCompatible(int n1, int n2) {
//		return checkVertexCompatible(n1, n2);
		Boolean value = m_vcc.get(n1, n2);
		if (value != null)
			return value.booleanValue();
		
		return true;
	}
	
	public boolean checkVertexCompatible(int n1, int n2) {
		Boolean value = m_vcc.get(n1, n2);
		if (value != null)
			return value.booleanValue();
		
		String l1 = m_qg.getNodeLabel(n1);
		if (!l1.startsWith("?")) { // not variable, ie. ground term
			String l2 = m_ig.getNodeLabel(n2);
			m_timings.start(Timings.GT);
			for (String label : m_qg.inEdgeLabels(n1)) {
				try {
					if (m_les.hasDocs(l2, label, l1)) {
						value = true;
						break;
					}
					else {
						value = false;
						break;
					}
				} catch (StorageException e) {
					e.printStackTrace();
				}
			}
			m_vcc.put(n1, n2, value);
			m_timings.end(Timings.GT);
			return value.booleanValue();
		}
		return true;
	}
}
