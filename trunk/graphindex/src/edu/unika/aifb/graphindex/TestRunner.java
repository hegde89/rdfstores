package edu.unika.aifb.graphindex;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;

public class TestRunner {
	private static final Logger log = Logger.getLogger(TestRunner.class);
	
	public static void main(String[] args) throws IOException, StorageException, InterruptedException, ExecutionException {
		if (args.length < 2) {
			log.info("Usage:");
			log.info(" TestRunner [vp|gi] <indexdirectory> <queriesfile> <outputdirectory> <repetitions>");
		}
		
		String type = args[0];
		String indexDir = args[1];
		String queriesFile = args[2];
		String outputDir = args[3];
		int reps = Integer.parseInt(args[4]);
		
		log.info("type: " + type);
		log.info("index: " + indexDir);
		log.info("queries: " + queriesFile);
		log.info("output: " + outputDir);
		log.info("repetitions:  " + reps);
		
		QueryLoader ql = new QueryLoader();
		List<Query> queries = ql.loadQueryFile(queriesFile);
		List<Query> ordered = new ArrayList<Query>(queries);
		log.info("loaded " + queries.size() + " queries");
		
		IQueryEvaluator qe = null;
		
		LuceneStorage ls = null;
		StructureIndexReader index = null;
		if (type.equals("vp")) {
			ls = new LuceneStorage(indexDir);
			ls.initialize(false, true);
			qe = new VPQueryEvaluator(ls);
		}
		else if (type.equals("gi")) {
			index = new StructureIndexReader(indexDir);
			index.setNumEvalThreads(2);
			index.getIndex().setTableCacheSize(1);
			index.getIndex().setDocumentCacheSize(1000);
			
			qe = index.getQueryEvaluator();
		}
		
		Map<String,Long> q2d  = new HashMap<String,Long>(); 
		
		for (int i = 1; i <= reps; i++) {
			log.info("run #" + i);
			log.info("--------------------------------------------------");
			
			Collections.shuffle(queries);
			
			String s = "";
			for (Query q : queries)
				s += q.getName() + " ";
			log.info("query order: " + s);
			log.info("--------------------------------------------------");
			
			for (Query q : queries) {
				System.gc();
				long start = System.currentTimeMillis();
				qe.evaluate(q);
				long duration = System.currentTimeMillis() - start;
				
				log.info(q.getName() + "\t" + duration);
				
				if (!q2d.containsKey(q.getName()))
					q2d.put(q.getName(), duration);
				else
					q2d.put(q.getName(), q2d.get(q.getName()) + duration);
			}
		}
		log.info("--------------------------------------------------");
		log.info("averages:");

		for (Query q : ordered) {
			log.info(q.getName() + "\t" + (double)q2d.get(q.getName()) / reps);
		}
		
		if (type.equals("vp")) {
			ls.close();
		}
		else if (type.equals("gi")) {
			index.close();
		}
	}
}
