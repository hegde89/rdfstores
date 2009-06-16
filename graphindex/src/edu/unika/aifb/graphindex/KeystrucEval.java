package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.graphindex.query.DirectExploringQueryEvaluator;
import edu.unika.aifb.graphindex.query.ExploringQueryEvaluator;
import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.IncrementalQueryEvaluator;
import edu.unika.aifb.graphindex.query.IndirectExploringQueryEvaluator;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.KeywordQueryLoader;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public class KeystrucEval {
	private static Logger log = Logger.getLogger(KeystrucEval.class);
	
	public static void main(String[] args) throws IOException, StorageException {
		OptionParser op = new OptionParser();
		op.accepts("so", "schema")
			.withRequiredArg().ofType(String.class);
		op.accepts("o", "direct")
			.withRequiredArg().ofType(String.class);
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
		
		if (!os.has("o") || !os.has("s")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String schemaDir = (String)os.valueOf("so");
		String indexDir = (String)os.valueOf("o");
		String system = (String)os.valueOf("s");
		String resultFile = (String)os.valueOf("rf");
		String dropCachesScript = (String)os.valueOf("dc");
		int reps = os.has("r") ? (Integer)os.valueOf("r") : 1;
		int cutoff = os.has("c") ? (Integer)os.valueOf("c") : -1;
		
		log.debug("schema dir: " + schemaDir);
		log.debug("dir: " + indexDir);
		log.debug("system: " + system);
		log.debug("result file: " + resultFile);
		log.debug("reps: " + reps);
		log.debug("cutoff: " + cutoff);
	
		List<QueryRun> queryRuns = new ArrayList<QueryRun>();
	
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
		
		log.info("loading queries");
		KeywordQueryLoader loader = new KeywordQueryLoader();
		List<KeywordQuery> queries = new ArrayList<KeywordQuery>();
		
		List<Stat> timingStats = Arrays.asList(Timings.STEP_KWSEARCH, Timings.STEP_EXPLORE, Timings.STEP_IQA, Timings.STEP_QA, Timings.TOTAL_QUERY_EVAL);
		List<Stat> counterStats = Arrays.asList(Counters.QT_QUERIES, Counters.QT_QUERY_SIZE, Counters.RESULTS);
		
//		queries.add(new KeywordQuery("qtest", "publication13 publicationAuthor graduatestudent52@department12.university0.edu publicationAuthor publication15 publicationAuthor graduatestudent7"));
//		queries.add(new KeywordQuery("qtest", "GraduateStudent51 takesCourse GraduateCourse3"));
//		queries.add(loader.loadQueryFile(queryFile).remove(0));
		queries.addAll(loader.loadQueryFile(queryFile));

		for (KeywordQuery q : queries) {
			if (qName != null && !q.getName().equals(qName))
				continue;
			
			for (int i = 0; i < reps; i++) {
				// clear caches
				if (dropCachesScript != null && new File(dropCachesScript).exists()) {
					log.info("clearing caches...");
					Runtime.getRuntime().exec(dropCachesScript);
				}
				else
					log.warn("no drop caches script, caches not cleared");
				
				Set<String> dataWarmup = new HashSet<String>();
				Set<String> keywordWarmup = new HashSet<String>();
				
//					if (new File(outputDirectory + "/data_warmup").exists()) {
//						dataWarmup = Util.readEdgeSet(outputDirectory + "/data_warmup");
//					}
//					else
//						log.warn("no data warmup");
//					
//					if (new File(outputDirectory + "/keyword_warmup").exists()) {
//						keywordWarmup = Util.readEdgeSet(outputDirectory + "/keyword_warmup");
//					}
//					else if (system.equals("spe"))
//						log.warn("no keyword warmup");
				
				log.info("opening and warming up...");
				StructureIndexReader reader = new StructureIndexReader(indexDir + "/sidx");
				reader.warmUp(dataWarmup);
				StructureIndexReader schemaReader = null;
				
				ExploringQueryEvaluator qe = null;
				StatisticsCollector collector = null;
				
				if (system.equals("direct")) {
					qe = new DirectExploringQueryEvaluator(reader, new KeywordSearcher(indexDir + "/keyword"));
				}
				else if (system.equals("indirect")) {
					schemaReader = new StructureIndexReader(schemaDir + "/sidx");
					
					qe = new IndirectExploringQueryEvaluator(schemaReader, new KeywordSearcher(schemaDir + "/keyword"),
						reader, new KeywordSearcher(indexDir + "/keyword"));
				}
				else {
					qe = null;
				}
				
				collector = reader.getIndex().getCollector();
				
				log.info("query: " + q.getName());
				log.info("keywords: " + q.getQuery());
				
				collector.reset();
				
				qe.evaluate(q.getQuery());
				
				Timings t = new Timings();
				Counters c = new Counters();
				collector.consolidate(t, c);
				
				t.logStats(timingStats);
				c.logStats(counterStats);
				
				queryRuns.add(new QueryRun(q.getName(), system, t, c));
				ps.print(q.getName() + ", " + system);
				for (Stat s : timingStats) {
					ps.print(", " + t.get(s));
				}
				for (Stat s : counterStats) {
					ps.print(", " + c.get(s));
				}
				ps.println();
				ps.flush();
				
				if (schemaReader != null) {
					schemaReader.getIndex().getExtensionManager().setMode(ExtensionManager.MODE_READONLY);
					schemaReader.close();
				}
				if (reader != null) {
					reader.getIndex().getExtensionManager().setMode(ExtensionManager.MODE_READONLY);
					reader.close();
				}
			}
		}
		
		ps.close();
		
	}
}
