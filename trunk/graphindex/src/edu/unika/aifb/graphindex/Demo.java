package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Demo {
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
			if (files.get(0).contains(".nt"))
				importer = new NTriplesImporter();
			else
				throw new Exception("file type unknown");
			
			importer.addImports(files);

			IndexCreator ic = new IndexCreator(new IndexDirectory(directory));
			ic.setImporter(importer);
			ic.setCreateDataIndex(true);
			ic.setCreateStructureIndex(true);
			ic.setCreateKeywordIndex(true);
			ic.setKWNeighborhoodSize(2);
			ic.setSIPathLength(1);
			ic.setSICreateDataExtensions(false);
			
			ic.create();
		}
		
		if (action.equals("test")) {
			IndexReader ir = new IndexReader(new IndexDirectory(directory));
			
			StructuredQuery q = new StructuredQuery("q1");
			q.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
			q.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?y");
//			q.addEdge("?t", "blah", "?x");
//			q.addEdge("?y", "blah", "?x");
//			q.addEdge("?y", "blah", "?z");
			q.setAsSelect("?x");
			
			PrunedQuery pq = new PrunedQuery(q, ir.getStructureIndex());
			
//			VPEvaluator eval = new VPEvaluator(ir);
//			log.debug(eval.evaluate(q));
			
			CombinedQueryEvaluator cqe = new CombinedQueryEvaluator(ir);
			cqe.setDoRefinement(true);
			cqe.evaluate(q);

			QueryEvaluator qe = new QueryEvaluator(ir);
			qe.evaluate(q);
		}
	}
}
