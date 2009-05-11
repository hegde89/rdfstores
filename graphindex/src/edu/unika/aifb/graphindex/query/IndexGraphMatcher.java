package edu.unika.aifb.graphindex.query;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public interface IndexGraphMatcher {
	public void initialize() throws StorageException;
	public GTable<String> match() throws StorageException;
	public void setQueryGraph(Query query, Graph<QueryNode> graph);
	
	public void setTimings(Timings t);
}