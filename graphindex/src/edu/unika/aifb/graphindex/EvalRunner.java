package edu.unika.aifb.graphindex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import edu.unika.aifb.graphindex.query.EntityLoader;
import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.IncrementalQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneNeighborhoodStorage;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;

public class EvalRunner {
	private static final Logger log = Logger.getLogger(EvalRunner.class);
	
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("qf", "query file")
			.withRequiredArg().ofType(String.class);
		op.accepts("q", "query name")
			.withRequiredArg().ofType(String.class);
		op.accepts("s", "system")
			.withRequiredArg().ofType(String.class);
		op.accepts("rf", "result file")
			.withRequiredArg().ofType(String.class);
		op.accepts("r", "repeats")
			.withRequiredArg().ofType(Integer.class);
		op.accepts("c", "cutoff")
			.withRequiredArg().ofType(Integer.class);
		op.accepts("sf", "start from specified query");
		op.accepts("dc", "drop caches script")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
		String outputDirectory = (String)os.valueOf("o");
		String system = (String)os.valueOf("s");
		String resultFile = (String)os.valueOf("rf");
		String dropCachesScript = (String)os.valueOf("dc");
		int reps = os.has("r") ? (Integer)os.valueOf("r") : 1;
		int cutoff = os.has("c") ? (Integer)os.valueOf("c") : -1;
		
		log.debug("dir: " + outputDirectory);
		log.debug("system: " + system);
		log.debug("result file: " + resultFile);
		log.debug("reps: " + reps);
		log.debug("cutoff: " + cutoff);

		String spDirectory = outputDirectory + "/sidx";
		String keywordIndexDirectory = outputDirectory + "/keyword";
		
//		List<QueryRun> queryRuns = new ArrayList<QueryRun>();

		PrintStream ps = System.out;
		if (resultFile != null) {
			if (!new File(resultFile).exists())
				new File(resultFile).createNewFile();
			ps = new PrintStream(new FileOutputStream(resultFile, true));
		}

		String queryFile = (String)os.valueOf("qf");
		String qName = (String)os.valueOf("q");
		
		if (queryFile == null) {
			log.error("no query file specified");
		}
		
		List<Stat> timingStats = Arrays.asList(Timings.STEP_ES, Timings.STEP_ASM, Timings.STEP_IM, Timings.STEP_DM, Timings.TOTAL_QUERY_EVAL);
		List<Stat> counterStats = Arrays.asList(Counters.QUERY_EDGES, Counters.QUERY_DEFERRED_EDGES, Counters.ES_RESULT_SIZE, 
			Counters.ASM_RESULT_SIZE, Counters.IM_INDEX_MATCHES, Counters.IM_PROCESSED_EDGES, Counters.DM_REM_EDGES, Counters.DM_REM_NODES, 
			Counters.DM_PROCESSED_EDGES, Counters.ES_PROCESSED_EDGES, Counters.RESULTS);

		List<Query> qs = new QueryLoader(null).loadQueryFile(queryFile);
		List<String> queryNames = new ArrayList<String>();
		for (Query q : qs)
			if (qName == null || qName.equals(q.getName()))
				queryNames.add(q.getName());

		for (String queryName : queryNames) {
			for (int i = 0; i < reps; i++) {
				// clear caches
				if (dropCachesScript != null && new File(dropCachesScript).exists()) {
					log.info("clearing caches...");
					Process p = Runtime.getRuntime().exec(dropCachesScript);
					p.waitFor();
				}
				else
					log.warn("no drop caches script, caches not cleared");
				
				log.info("opening ...");
				StructureIndexReader reader = null;
				LuceneStorage ls = null;
				IQueryEvaluator qe = null;
				StatisticsCollector collector = null;
				if (system.equals("sp")) {
					reader = new StructureIndexReader(outputDirectory);
					
					qe = new QueryEvaluator(reader);
					collector = reader.getIndex().getCollector();
				}
				else if (system.equals("spe")) {
					reader = new StructureIndexReader(spDirectory);
					
					qe = new IncrementalQueryEvaluator(reader, new EntityLoader(outputDirectory + "/vp"),
						new LuceneNeighborhoodStorage(outputDirectory + "/neighborhood"));
					((IncrementalQueryEvaluator)qe).setCutoff(cutoff);
					collector = reader.getIndex().getCollector();
				}
				else if (system.equals("vp")) {
					collector = new StatisticsCollector();
					ls = new LuceneStorage(outputDirectory);
					ls.initialize(false, true);
					
					qe = new VPQueryEvaluator(ls, collector);
					reader = null;
				}
				else {
					qe = null;
					reader = null;
					collector = null;
				}
				
				log.info("loading queries");
				QueryLoader loader = new QueryLoader(reader != null ? reader.getIndex() : null);
				List<Query> queries = loader.loadQueryFile(queryFile);
				
				boolean startFound = false;
				for (Query q : queries) {
					if (!startFound && queryName != null && !q.getName().equals(queryName))
						continue;
					startFound = os.has("sf");
					log.info("query: " + q.getName());
					
					collector.reset();
					
					if (system.equals("sp") || system.equals("spe"))
						q.trimPruning(reader.getIndex().getPathLength());
					
					List<String[]> results = qe.evaluate(q);
					log.info("query " + q.getName() + ": " + results.size() + " results");
					
					Timings t = new Timings();
					Counters c = new Counters();
					collector.consolidate(t, c);
					
					t.logStats(timingStats);
					c.logStats(counterStats);
					
//					queryRuns.add(new QueryRun(q.getName(), system, t, c));
					ps.print(q.getName() + "," + system);
					for (Stat s : timingStats) {
						ps.print("," + t.get(s));
					}
					for (Stat s : counterStats) {
						ps.print("," + c.get(s));
					}
					ps.println();
					ps.flush();
				}
				
				if (reader != null) {
					reader.getIndex().getExtensionManager().setMode(ExtensionManager.MODE_READONLY);
					reader.close();
				}
				
				if (ls != null) {
					ls.close();
				}
			}
		}
		
		ps.close();
		
		
	}

}
