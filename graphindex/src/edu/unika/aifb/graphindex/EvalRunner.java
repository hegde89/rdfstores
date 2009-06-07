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

import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.IncrementalQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;

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
		op.accepts("sf", "start from specified query");
		
		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
		String outputDirectory = (String)os.valueOf("o");
		String system = (String)os.valueOf("s");
		String resultFile = (String)os.valueOf("f");
		int reps = os.has("r") ? (Integer)os.valueOf("r") : 1;

		String spDirectory = outputDirectory + "/sidx";
		String keywordIndexDirectory = outputDirectory + "/keyword";
		
		log.debug(Util.memory());
		
		List<QueryRun> queryRuns = new ArrayList<QueryRun>();

		if (action.equals("query") || action.equals("spquery")) {
			String queryFile = (String)os.valueOf("qf");
			String queryName = (String)os.valueOf("q");
			
			if (queryFile == null) {
				log.error("no query file specified");
			}

			for (int i = 0; i < reps; i++) {
				// clear caches
				
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
					qe = new IncrementalQueryEvaluator(reader, keywordIndexDirectory);
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
				
				QueryLoader loader = new QueryLoader(reader != null ? reader.getIndex() : null);
				List<Query> queries = loader.loadQueryFile(queryFile);
				
				boolean startFound = false;
				for (Query q : queries) {
					if (!startFound && queryName != null && !q.getName().equals(queryName))
						continue;
					startFound = os.has("sf");
					log.info("query: " + q.getName());
					
					collector.reset();
					
					List<String[]> results = qe.evaluate(q);
					log.info("query " + q.getName() + ": " + results.size() + " results");
					
					Timings t = new Timings();
					Counters c = new Counters();
					collector.consolidate(t, c);
					
					t.logStats();
					c.logStats();
					
					queryRuns.add(new QueryRun(q.getName(), system, t, c));
					System.out.print(q.getName() + ", " + system);
					for (Stat s : Timings.stats) {
						System.out.print(", " + t.get(s));
					}
					for (Stat s : Counters.stats) {
						System.out.print(", " + c.get(s));
					}
					System.out.println();
				}
				if (reader != null) {
					reader.getIndex().getExtensionManager().setMode(ExtensionManager.MODE_READONLY);
					reader.close();
				}
				if (ls != null) {
					ls.close();
				}
			}
			
			PrintStream ps = System.out;
			if (resultFile != null) {
				ps = new PrintStream(new FileOutputStream(resultFile));
			}
			
			String comma = "";
			// column names
			ps.print("query, system");
			for (Stat s : Timings.stats) {
				ps.print(", t_" + s.name);
			}
			for (Stat s : Counters.stats) {
				ps.print(", c_" + s.name);
			}
			ps.println();
			
			for (QueryRun qr : queryRuns) {
				ps.print(qr.getName() + ", " + qr.getSystem());
				for (Stat s : Timings.stats) {
					ps.print(", " + qr.getTimings().get(s));
				}
				for (Stat s : Counters.stats) {
					ps.print(", " + qr.getCounters().get(s));
				}
				ps.println();
			}
		}
	}

}
