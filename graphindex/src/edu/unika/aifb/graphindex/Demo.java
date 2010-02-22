package edu.unika.aifb.graphindex;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.keyword.ExploringKeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.StructureIndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
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
				importer = new NxImporter();
			else if (files.get(0).contains(".rdf"))
				importer = new RDFImporter();
			else
				throw new Exception("file type unknown");
			
			importer.setIgnoreDataTypes(true);
			importer.addImports(files);

			IndexCreator ic = new IndexCreator(new IndexDirectory(directory));
			
			// the importer is the data source
			ic.setImporter(importer);
			
			// create a data index (default: true)
			ic.setCreateDataIndex(true);
			
			// create structure index (default: true)
			ic.setCreateStructureIndex(false);
			
			// create keyword index (default: true)
			ic.setCreateKeywordIndex(true);
						
			// set neighborhood size to 2 (default: 0)
	//		ic.setKWNeighborhoodSize(2);
			
			// set structure index path length to 1 (default: 1)
	//		ic.setSIPathLength(1);
			
			// include data values in structure index (not graph) (default: true)
	//		ic.setStructureBasedDataPartitioning(true);
			
	//		ic.setSICreateDataExtensions(true);
			
	//		ic.setOption(IndexConfiguration.TRIPLES_ONLY, true);
			
			// create index
			ic.create();
		}
		
		if (action.equals("test")) {
			// an index is accessed through an IndexReader object, which is passed
			// to all evaluators and searchers
			IndexReader ir = new IndexReader(new IndexDirectory(directory));
			
			
			// print all object properties in the data set
			// IndexReader has other methods which report information about the data,
			// e.g. cardinalities (some are not yet implemented)
			System.out.println(ir.getObjectProperties());
			
			// quad/triple retrieval with arbitrarily specified positions
			DataIndex di = ir.getDataIndex();
			Table<String> table = di.getQuads(null, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, null);
			System.out.println("type property quads: " + table);
			
			// create a structured query
			StructuredQuery q = new StructuredQuery("q1");
			
			// add edges to the query
			q.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
			q.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?y");
			
			// set select variables (currently has to be done after adding edges)
			q.setAsSelect("?x");

			// use an VPEvaluator to execute the query
			// VPEvaluator uses the data index only
			VPEvaluator eval = new VPEvaluator(ir);
			System.out.println(eval.evaluate(q));
			
			// use an CombinedEvaluator to execute the query
			// CombinedEvaluator uses both the data and the structure index and
			// will automatically prune queries
	//		CombinedQueryEvaluator cqe = new CombinedQueryEvaluator(ir);
	//		System.out.println(cqe.evaluate(q));
			
			// retrieve only matches on the index graph
	//		StructureIndexGraphMatcher sigm = new StructureIndexGraphMatcher(ir);
	//		System.out.println(sigm.evaluate(q));
			
			// queries can be pruned like this
			// pruning is dependent on the structure index and the path length used
	//		PrunedQuery pq = new PrunedQuery(q, ir.getStructureIndex());
	//		System.out.println(pq.getQueryGraph().edgeCount() + " -> " + pq.getPrunedQueryGraph().edgeCount());
			
			// a keyword query, DirectExploringQueryEvaluator is the only currently usable for keyword queries
			KeywordQuery kq = new KeywordQuery("q1", "Publication0 publicationAuthor GraduateStudent1@Department10.University0.edu");
			KeywordQueryEvaluator kwEval = new ExploringKeywordQueryEvaluator(ir);
			System.out.println(kwEval.evaluate(kq).toDataString());
		}
	}
}
