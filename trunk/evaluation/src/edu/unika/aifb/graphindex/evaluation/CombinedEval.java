package edu.unika.aifb.graphindex.evaluation;


import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.plan.QueryPlanEmitter;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

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
		
		List<Stat> timingStats = Arrays.asList(Timings.STEP_IM, Timings.STEP_DM, Timings.LOAD_DOCIDS, Timings.LOAD_DOC, Timings.LOAD_DATA_ITEM, Timings.STEP_CB_REFINE, Timings.TOTAL_QUERY_EVAL);
		List<Stat> counterStats = Arrays.asList(Counters.QUERY_EDGES,  Counters.DM_REM_EDGES,	Counters.DM_REM_NODES, 
			Counters.DM_PROCESSED_EDGES, Counters.RESULTS);

		IndexDirectory idxDirectory = new IndexDirectory(outputDirectory);
		
		QueryLoader ql = new QueryLoader();
		List<StructuredQuery> qs = ql.loadQueryFile(queryFile);
	
		for (StructuredQuery q : qs) {
			if (qName != null && !q.getName().equals(qName))
				continue;
			
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
				
				IndexReader reader = new IndexReader(idxDirectory);
				StatisticsCollector collector = reader.getCollector();
				StructuredQueryEvaluator qe = null;
				
				if (system.equals("sp")) {
					qe = new QueryEvaluator(reader);
				}
				else if (system.equals("spc")) {
					qe = new QueryPlanEmitter(reader);
					((QueryPlanEmitter)qe).setDoRefinement(true);
				}
				else if (system.equals("vp")) {
					qe = new QueryPlanEmitter(reader);
					((QueryPlanEmitter)qe).setDoRefinement(false);
				}
				else {
					qe = null;
					reader = null;
					collector = null;
				}
				
				log.info("query: " + q.getName());
				
				collector.reset();
				
				Table<String> results = qe.evaluate(q);
				log.info("query " + q.getName() + ": " + results.rowCount() + " results");
				
				Timings t = new Timings();
				Counters c = new Counters();
				collector.consolidate(t, c);
				
				t.logStats(timingStats);
				c.logStats(counterStats);
				
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
		}
		
		ps.close();
		
		
	}
}
