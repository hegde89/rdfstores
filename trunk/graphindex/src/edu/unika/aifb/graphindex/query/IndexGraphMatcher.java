package edu.unika.aifb.graphindex.query;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;

public interface IndexGraphMatcher {
	public void initialize() throws StorageException;
	public void match() throws StorageException;
	public void setQueryExecution(QueryExecution qe);
	
	public void setTimings(Timings t);
}