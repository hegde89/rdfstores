package edu.unika.aifb.graphindex.query;

import java.util.List;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public interface IndexMatchesValidator {
	public List<String[]> validateIndexMatches(Query query, Graph<QueryNode> queryGraph, GTable<String> indexMatches, List<String> selectVariables) throws StorageException;
	public void clearCaches();
}
