package edu.unika.aifb.querygenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.querygenerator.query.SQuery;

public abstract class AbstractGenerator implements Generator {

	protected int maxAtom;
	protected int maxVar;
	protected int numQuery;
	protected IndexReader m_idxReader;
	protected Set<SQuery> queries;
	protected PrintWriter out;
	protected Set<String> centerElements;
	protected int numAtom;
	protected int numVar;
	protected int numDVar;
	
	
	public AbstractGenerator(String indexDir, String outputFile, int maxAtom, int maxVar, int numQuery) throws StorageException, IOException {
		m_idxReader = new IndexReader(new IndexDirectory(indexDir));

		this.maxAtom = maxAtom;
		this.maxVar = maxVar; 
		this.numQuery = numQuery;
		this.out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
		this.queries = new HashSet<SQuery>();
		this.centerElements = new HashSet<String>();
		this.numAtom = 0;
		this.numVar = 0;
		this.numDVar = 0;
	}
	
	public void generateQueries() throws IOException {
		int maxDoc = dataSearcher.maxDoc();
		Random r = new Random();
		while(queries.size() < numQuery) {
			SQuery sq = generateQuery(maxDoc, r);
			if(sq != null){
				sq.generateVariables();
				if(!queries.contains(sq)) {
					queries.add(sq);
				}
			}	
		}
	}
	
	public void generateQueriesWithoutResults() throws IOException {
		int maxDoc = dataSearcher.maxDoc();
		Random r = new Random();
		while(queries.size() < numQuery) {
			SQuery sq = generateQueryWithoutResults(maxDoc, r);
			if(sq != null){
				sq.generateVariables();
				if(!queries.contains(sq)) {
					queries.add(sq);
				}
			}	
		}
	}
	
	public void writeQueries() {
		int i = 1;
		for(SQuery sq : queries) {
			out.println("query:  " + "q" + i );
			out.println("select: " + sq.getVariables(maxVar));
			out.println(sq.getQuery());
			i++;
			numAtom += sq.getNumAtom();
			numVar += sq.getNumVar();
			numDVar += sq.getNumDvar();
		}
		
		
		out.println();
		out.println("Average Number of Atoms: " + numAtom/numQuery);
		out.println("Average Number of Variables: " + numVar/numQuery);
		out.println("Average Number of Distinguished Variables: " + numDVar/numQuery);
	}

	public void close() throws StorageException {
		out.close();
		structureIndexReader.close();
	}
	
}
