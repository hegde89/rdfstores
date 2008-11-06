package edu.unika.aifb.graphindex.query;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryFeasibilityChecker implements FeasibilityChecker {

	private VCompatibilityCache m_vcc;
	private Timings m_timings;
	private ExtensionStorage m_les;
	private Graph<QueryNode> m_qg;
	private Graph<String> m_ig;
	private static final Logger log = Logger.getLogger(QueryFeasibilityChecker.class);

	public QueryFeasibilityChecker(Graph<QueryNode> queryGraph, Graph<String> indexGraph, VCompatibilityCache cache, Timings t, ExtensionStorage les) {
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
		QueryNode qn = m_qg.getNode(n1);

		if (qn.hasGroundTerms() && !qn.hasVariables()) {
			m_timings.start(Timings.GT);

			String l2 = m_ig.getNode(n2);

			boolean allFound = true;
			for (String l1 : qn.getGroundTerms()) {
				if (!m_vcc.get(l1, l2)) {
					allFound = false;
					break;
				}
			}
			
			m_timings.end(Timings.GT);
			return allFound;
		}
		
		return true;
	}
}
