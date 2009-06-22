package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.query.EntityLoader;
import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.IncrementalQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneNeighborhoodStorage;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;

public class CombinedEval {
	private static final Logger log = Logger.getLogger(CombinedEval.class);
	
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
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
		op.accepts("dc", "drop caches script")
			.withRequiredArg().ofType(String.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String outputDirectory = (String)os.valueOf("o");
		String system = (String)os.valueOf("s");
		String resultFile = (String)os.valueOf("rf");
		String dropCachesScript = (String)os.valueOf("dc");
		int reps = os.has("r") ? (Integer)os.valueOf("r") : 1;
		
		log.debug("dir: " + outputDirectory);
		log.debug("system: " + system);
		log.debug("result file: " + resultFile);
		log.debug("reps: " + reps);

		String spDirectory = outputDirectory + "/sidx";
		
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
		
		List<Stat> timingStats = Arrays.asList(Timings.STEP_IM, Timings.STEP_DM, Timings.TOTAL_QUERY_EVAL);
		List<Stat> counterStats = Arrays.asList(Counters.QUERY_EDGES,  Counters.DM_REM_EDGES,	Counters.DM_REM_NODES, 
			Counters.DM_PROCESSED_EDGES, Counters.RESULTS);

		QueryLoader ql = new QueryLoader(null);
		List<Query> qs = ql.loadQueryFile(queryFile);
	
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
				else if (system.equals("spc")) {
					reader = new StructureIndexReader(spDirectory);
					ls = new LuceneStorage(outputDirectory + "/vp");
					ls.initialize(false, true);
					
					collector = reader.getIndex().getCollector();

					qe = new CombinedQueryEvaluator(reader, ls);
					((CombinedQueryEvaluator)qe).setDoRefinement(true);
				}
				else if (system.equals("vp")) {
					reader = new StructureIndexReader(spDirectory);
					ls = new LuceneStorage(outputDirectory + "/vp");
					ls.initialize(false, true);
					
					collector = reader.getIndex().getCollector();

					qe = new CombinedQueryEvaluator(reader, ls);
					((CombinedQueryEvaluator)qe).setDoRefinement(false);
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
					
					if (system.equals("spc") || system.equals("sp"))
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
