package edu.unika.aifb.query.generator;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.query.query.SQuery;

public interface Generator {
	
	public final String FIELD_SRC = "src";
	public final String FIELD_EDGE = "edge";
	public final String FIELD_DST = "dst";
	public final String FIELD_CONSTANT = "con";
	
	public final String FIELD_BLOCK = "block";
	public final String FIELD_ELE = "ele";
	public final String FIELD_LIT = "lit";
	
	public SQuery generateQuery(int maxDoc, Random r) throws IOException;
	public SQuery generateQueryWithoutResults(int maxDoc, Random r) throws IOException;
	public void generateQueries() throws IOException;
	public void generateQueriesWithoutResults() throws IOException;
	
	public void writeQueries();
	
	public void close() throws StorageException;

}
