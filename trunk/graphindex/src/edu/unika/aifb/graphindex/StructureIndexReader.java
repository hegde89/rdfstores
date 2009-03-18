package edu.unika.aifb.graphindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Util;

public class StructureIndexReader {
	private StructureIndex m_index;
	private List<Graph<String>> m_graphs;
	private List<String> m_graphNames;
	private QueryEvaluator m_evaluator;
	
	private int m_configEvalThreads = 15;
	private QueryLoader m_loader;
	
	private static final Logger log = Logger.getLogger(StructureIndexReader.class);
	
	public StructureIndexReader(String directory) throws StorageException, IOException {
		m_index = new StructureIndex(directory, false, true);
		m_graphs = new ArrayList<Graph<String>>();
		m_graphNames = new ArrayList<String>();
		loadIndexGraphs();
		
		File edgeSetFile = new File(directory + "/forward_edgeset");
		if (edgeSetFile.exists()) {
			Set<String> edgeSet = Util.readEdgeSet(edgeSetFile);
			log.debug("fw: " + edgeSet);
			m_index.setForwardEdges(edgeSet);
		}
		edgeSetFile = new File(directory + "/backward_edgeset");
		if (edgeSetFile.exists()) {
			Set<String> edgeSet = Util.readEdgeSet(edgeSetFile);
			log.debug("bw: " + edgeSet);
			m_index.setBackwardEdges(edgeSet);
		}
	}
	
	private void loadIndexGraphs() throws StorageException {
		for (String name : m_index.getGraphManager().getStoredGraphs()) {
			m_graphNames.add(name);
//			m_graphs.add(new Graph<String>(m_index.getGraphManager().graph(name)));
		}
	}
	
	public List<String> getGraphNames() {
		return m_graphNames;
	}
	
	public List<Graph<String>> getIndexGraphs() {
		return m_graphs;
	}
	
	public QueryEvaluator getQueryEvaluator() throws StorageException {
		if (m_evaluator == null)
			m_evaluator = new QueryEvaluator(this);
		return m_evaluator;
	}
	
	public QueryLoader getQueryLoader() {
		if (m_loader == null)
			m_loader = new QueryLoader(getIndex());
		return m_loader;
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
