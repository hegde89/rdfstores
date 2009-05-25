package edu.unika.aifb.graphindex.query;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public interface IQueryEvaluator {
	public List<String[]> evaluate(Query q) throws StorageException;
	public void clearCaches() throws StorageException;
	public long[] getTimings();
}
