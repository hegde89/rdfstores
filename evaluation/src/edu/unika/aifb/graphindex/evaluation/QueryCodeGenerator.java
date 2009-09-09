package edu.unika.aifb.graphindex.evaluation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.plan.QueryPlanEmitter;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Stat;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryCodeGenerator {
	
	private static final Logger log = Logger.getLogger(QueryCodeGenerator.class);
	
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
		
		log.debug("dir: " + outputDirectory);
		log.debug("system: " + system);

		PrintStream ps = System.out;

		String queryFile = (String)os.valueOf("qf");
		String qName = (String)os.valueOf("q");
		
		if (queryFile == null) {
			log.error("no query file specified");
		}
		
		IndexDirectory idxDirectory = new IndexDirectory(outputDirectory);
		
		QueryLoader ql = new QueryLoader();
		List<StructuredQuery> qs = ql.loadQueryFile(queryFile);
	
		String output = "";
		
		for (StructuredQuery q : qs) {
			if (qName != null && !q.getName().equals(qName))
				continue;
			
			
			IndexReader reader = new IndexReader(idxDirectory);
			QueryPlanEmitter qe = null;
			
			if (system.equals("sp")) {
//				qe = new QueryEvaluator(reader);
			}
			else if (system.equals("spc")) {
				qe = new QueryPlanEmitter(reader);
				qe.setDoRefinement(true);
			}
			else if (system.equals("vp")) {
				qe = new QueryPlanEmitter(reader);
				qe.setDoRefinement(false);
			}
			else {
				qe = null;
				reader = null;
			}
			
			output += qe.getQueryCode(q);
		}
		
		System.out.println(output);
	}
}
