package edu.unika.aifb.graphindex;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.unika.aifb.graphindex.storage.StorageException;

public class MergedIndexList<G> {
	
	private IndexMerger<G> m_merger;
	private LinkedList<G> m_graphs;
	private Comparator<G> m_comparator;
	
	public MergedIndexList(IndexMerger<G> merger, Comparator<G> comparator) {
		m_merger = merger;
		m_comparator = comparator;
		m_graphs = new LinkedList<G>();
	}
	
	private int searchList(List<G> c, G g) {
		int idx = Collections.binarySearch(c, g, m_comparator);
		
		idx = idx >= 0 ? idx : -1 - idx;

		// If a collection has multiple entries with the same value
		// Collections.binarySearch makes no guarantee which one will be returned.
		// This makes sure idx points to the left-most entry.
		for (int i = idx - 1; i >= 0; i--) {
			if (m_comparator.compare(c.get(i), g) > 0) {
				idx = i + 1;
				break;
			}
		}
		
		return idx;
	}
	
	public void add(G graph) throws StorageException {
		int idx = searchList(m_graphs, graph);

		boolean merged = false;
		
		if (idx < m_graphs.size()) {
			while (idx < m_graphs.size()) {
				G lg = m_graphs.get(idx);
				if (m_comparator.compare(lg, graph) == 0 && m_merger.merge(graph, lg)) {
					merged = true;
					break;
				}
				idx++;
			}
		}
		
		if (!merged) {
			m_graphs.add(idx, graph);
		}
	}
	
	public List<G> getList() {
		return m_graphs;
	}
}
