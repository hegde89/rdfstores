package edu.unika.aifb.graphindex.query;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;

public interface IQueryEvaluator {
	public void evaluate(Query q) throws StorageException, IOException, InterruptedException, ExecutionException;
}
