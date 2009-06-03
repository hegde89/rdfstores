package edu.unika.aifb.graphindex.query;

import java.util.List;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public interface IndexMatchesValidator {
	public void setQueryExecution(QueryExecution qe);
	public void validateIndexMatches() throws StorageException;
	public void clearCaches();
	public Timings getTimings();
	public void setTimings(Timings timings);
	public void setCounters(Counters c);
}
