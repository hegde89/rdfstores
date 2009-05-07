package edu.unika.aifb.graphindex.indexing;

import java.util.HashMap;
import java.util.Map;

import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.GraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * An implementation of IndexMerger for 1-index graphs.
 * 
 * @author gl
 *
 */
class OneIndexMerger implements IndexGraphMerger<IndexGraph> {

	private Map<String,String> m_mergeMap;

	public OneIndexMerger() {
		m_mergeMap = new HashMap<String,String>();
	}

	public boolean merge(IndexGraph small, IndexGraph large) throws StorageException {
		GraphMatcher matcher = new GraphMatcher(small, large, false, new FeasibilityChecker() {
			public boolean checkVertexCompatible(int n1, int n2) {
				return true;
			}

			public boolean isEdgeCompatible(IndexEdge e1, IndexEdge e2) {
				return e1.getLabel().equals(e2.getLabel());
			}

			public boolean isVertexCompatible(int n1, int n2) {
				return true;
			}
		
		});
		
		if (!matcher.isIsomorphic())
			return false;

		FastIndexBuilder.log.debug(small + " isomorphic to " + large);
		
		for (VertexMapping vm : matcher) {
			for (String v : large.nodeLabels()) {
				// TODO verify true or false (DiGraphMatcher constructor parameter order may have changed)
//					m_em.extension(v).mergeExtension(m_em.extension(iso.getVertexCorrespondence(v, false)));
				m_mergeMap.put(vm.getVertexCorrespondence(v, false), v);
			}
			break;
		}
		
		return true;
	}
	
	public Map<String,String> getMergeMap() {
		return m_mergeMap;
	}
}