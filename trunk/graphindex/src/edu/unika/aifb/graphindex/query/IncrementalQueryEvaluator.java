package edu.unika.aifb.graphindex.query;

import java.util.List;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public class IncrementalQueryEvaluator implements IQueryEvaluator {

	public IncrementalQueryEvaluator() {
		
	}
	
	public List<String[]> evaluate(Query q) throws StorageException {
		Graph<QueryNode> queryGraph = q.getGraph();
		
		// step 1: entity search
		
		// step 2: approximate structure matching
		
		// step 3: structure-based refinement
		
		// step 4: result computation 
		
		return null;
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}
