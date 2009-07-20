package edu.unika.aifb.querygenerator;

import java.io.IOException;

import edu.unika.aifb.graphindex.storage.StorageException;

public class Main {
	
//	public static final String outputFile = "D:/QueryGenerator/GraphQuery.txt";
//	
//	public static final String indexDir = "D:/QueryGenerator/output/lubm/l1r";	
//		
//	public static void main(String[] args) throws StorageException, IOException {
//		Generator qg = new GraphQueryGenerator(indexDir, outputFile, 30, 4, 100);
//		qg.generateQueriesWithoutResults();
//		qg.writeQueries();
//		qg.close();
//	}
	
	public static String outputDir;
	
	public static String indexDir;	
		
	public static void main(String[] args) throws StorageException, IOException {
		if (args.length != 3) {
			System.err.println("java Main dataset(String) queryShape(String) 'result or no_result'");
			return;
		}
		if(args[0].equals("lubm50")) {
			outputDir = "/local/users/btc/lei/query/lubm50/";
			indexDir = "/local/users/btc/lei/lubm50/output/lubm/l1r";	
			if (args[1].equals("atom")) {
				if(args[2].equals("result")) {
					Generator qg = new AtomQueryGenerator(indexDir, outputDir + "AtomQuery.txt", 1, 1, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new AtomQueryGenerator(indexDir, outputDir + "AtomQuery_noResult.txt", 1, 1, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("path")) {
				if(args[2].equals("result")) {
					Generator qg = new PathQueryGenerator(indexDir, outputDir + "PathQuery.txt", 20, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new PathQueryGenerator(indexDir, outputDir + "PathQuery_noResult.txt", 20, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("entity")) {
				if(args[2].equals("result")) {
					Generator qg = new EntityQueryGenerator(indexDir, outputDir + "EntityQuery.txt", 20, 1, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new EntityQueryGenerator(indexDir, outputDir + "EntityQuery_noResult.txt", 20, 1, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("star")) {
				if(args[2].equals("result")) {
					Generator qg = new StarQueryGenerator(indexDir, outputDir + "StarQuery.txt", 20, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new StarQueryGenerator(indexDir, outputDir + "StarQuery_noResult.txt", 20, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("graph")) {
				if(args[2].equals("result")) {
					Generator qg = new GraphQueryGenerator(indexDir, outputDir + "GraphQuery.txt", 30, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new GraphQueryGenerator(indexDir, outputDir + "GraphQuery_noResult.txt", 30, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
		}
		else if(args[0].equals("dblp")) {
			outputDir = "/local/users/btc/lei/query/dblp/";
			indexDir = "/local/users/btc/lei/dblp/output/dblp/l1r";	
			if (args[1].equals("atom")) {
				if(args[2].equals("result")) {
					Generator qg = new AtomQueryGenerator(indexDir, outputDir + "AtomQuery.txt", 1, 1, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new AtomQueryGenerator(indexDir, outputDir + "AtomQuery_noResult.txt", 1, 1, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("path")) {
				if(args[2].equals("result")) {
					Generator qg = new PathQueryGenerator(indexDir, outputDir + "PathQuery.txt", 20, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new PathQueryGenerator(indexDir, outputDir + "PathQuery_noResult.txt", 20, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("entity")) {
				if(args[2].equals("result")) {
					Generator qg = new EntityQueryGenerator(indexDir, outputDir + "EntityQuery.txt", 20, 1, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new EntityQueryGenerator(indexDir, outputDir + "EntityQuery_noResult.txt", 20, 1, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("star")) {
				if(args[2].equals("result")) {
					Generator qg = new StarQueryGenerator(indexDir, outputDir + "StarQuery.txt", 20, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new StarQueryGenerator(indexDir, outputDir + "StarQuery_noResult.txt", 20, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
			if (args[1].equals("graph")) {
				if(args[2].equals("result")) {
					Generator qg = new GraphQueryGenerator(indexDir, outputDir + "GraphQuery.txt", 30, 4, 100);
					qg.generateQueries();
					qg.writeQueries();
					qg.close();
				}
				if(args[2].equals("no_result")) {
					Generator qg = new GraphQueryGenerator(indexDir, outputDir + "GraphQuery_noResult.txt", 30, 4, 100);
					qg.generateQueriesWithoutResults();
					qg.writeQueries();
					qg.close();
				}
			}
		}
	}

}
