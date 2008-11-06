package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class StructureIndexReader {
	private StructureIndex m_index;
	private List<Graph<String>> m_graphs;
	private QueryEvaluator m_evaluator;
	
	private int m_configEvalThreads = 10;
	
	public StructureIndexReader(String directory) throws StorageException {
		m_index = new StructureIndex(directory, false, true);
		m_graphs = new ArrayList<Graph<String>>();
		loadIndexGraphs();
	}
	
	private void loadIndexGraphs() throws StorageException {
		for (String name : m_index.getGraphManager().getStoredGraphs()) {
			m_graphs.add(new Graph<String>(m_index.getGraphManager().graph(name)));
		}
	}
	
	public List<Graph<String>> getIndexGraphs() {
		return m_graphs;
	}
	
	public QueryEvaluator getQueryEvaluator() {
		if (m_evaluator == null)
			m_evaluator = new QueryEvaluator(this);
		return m_evaluator;
	}
	
	public StructureIndex getIndex() {
		return m_index;
	}
	
	public void close() throws StorageException {
		m_index.close();
	}
	
	public int getNumEvalThreads() {
		return m_configEvalThreads;
	}
	
	public void setNumEvalThreads(int n) {
		m_configEvalThreads = n;
	}
}
