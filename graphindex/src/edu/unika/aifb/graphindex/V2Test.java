package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.DirectExploringQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class V2Test {
	private static final Logger log = Logger.getLogger(V2Test.class);

	private static class Value<T> {
		public T val;
		
		public Value(T val) {
			this.val = val;
		}
	}
	
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");

		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
		String directory = (String)os.valueOf("o");
		
		IndexDirectory dir = new IndexDirectory(directory);
		
		if (action.equals("create")) {
			List<String> files = os.nonOptionArguments();

			if (files.size() == 1) {
				// check if file is a directory, if yes, import all files in the directory
				File f = new File(files.get(0));
				if (f.isDirectory()) {
					files = new ArrayList<String>();	
					for (File file : f.listFiles())
						if (!file.getName().startsWith("."))
							files.add(file.getAbsolutePath());
				}
			}

			Importer importer;
			if (files.get(0).contains(".nt") || !files.get(0).contains("."))
				importer = new NxImporter();
			else if (files.get(0).contains(".owl"))
				importer = new OntologyImporter();
			else if (files.get(0).contains(".rdf") || files.get(0).contains(".xml"))
				importer = new RDFImporter();
			else
				throw new Exception("file type unknown");
			
			importer.addImports(files);
			
			IndexCreator ic = new IndexCreator(dir);
			ic.setImporter(importer);
			ic.setKWNeighborhoodSize(3);
			
			ic.create();
		}
		
		if (action.equals("importtest")) {
			final Value<Integer> triples = new Value<Integer>(0);
			Importer importer = new NxImporter();
			importer.addImport("/data/datasets/btc2009/btc-2009-chunk-001");
			importer.setTripleSink(new TripleSink() {
				public void triple(String subject, String property, String object, String context) {
					triples.val++;
					
					if (triples.val % 500000 == 0) {
						log.debug(triples.val);
						log.debug(subject + " "+ property + " " + object + " | " + context);
					}
				}
			});
			
			importer.doImport();
			log.debug(triples.val);
		}
		
		if (action.equals("test")) {
			IndexReader ir = new IndexReader(dir);
			
			StructuredQuery q = new StructuredQuery("q1");
//			q.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
//			q.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?y");
//			q.setAsSelect("?x");
			
//			PrunedQuery pq = new PrunedQuery(q, ir.getStructureIndex());
			
			VPEvaluator eval = new VPEvaluator(ir);
//			log.debug(eval.evaluate(q));
			
//			CombinedQueryEvaluator cqe = new CombinedQueryEvaluator(ir);
//			cqe.setDoRefinement(true);
//			cqe.evaluate(q);
//
//			QueryEvaluator qe = new QueryEvaluator(ir);
//			qe.evaluate(q);
			
			q = new StructuredQuery("q");
			q.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");
			q.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?y");
			q.addEdge("?y", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", "Course35");
			q.setAsSelect("?x");
			q.setAsSelect("?y");
//			eval.evaluate(q);

			KeywordQuery kq = new KeywordQuery("q1", "GraduateStudent36 teachingAssistantOf");
//			KeywordQuery kq = new KeywordQuery("q", "UndergraduateStudent141@Department8.University0.edu takesCourse Course17 memberOf Department8");
			
			HybridQuery hq = new HybridQuery("h1", q, kq);
			
			ExploringHybridQueryEvaluator hy = new ExploringHybridQueryEvaluator(ir);
			hy.evaluate(hq);
			
			StructuredQuery q2 = new StructuredQuery("asd");
			q2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teachingAssistantOf", "?x2");
			q2.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?x2");
			q2.addEdge("?x1", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", "GraduateStudent36");
            q2.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor");
            q2.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", "?y");
            q2.addEdge("?y", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", "Course35");
            q2.setAsSelect("?x");
            q2.setAsSelect("?y");
            q2.setAsSelect("?x1");
            q2.setAsSelect("?x2");

            log.debug(eval.evaluate(q2).toDataString());
		}
		
	}
}
