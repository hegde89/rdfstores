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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix.Column.Row;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocCollector;
import org.apache.lucene.search.TopDocs;

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
import edu.unika.aifb.graphindex.ranking.TSSimilarity;
import edu.unika.aifb.graphindex.searcher.keyword.ExploringKeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.StructureIndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Demo {
	
	private static boolean tsrank;
	
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("tsrank", "Use text and structure ranking");
		
		
		OptionSet os = op.parse(args);
		
		
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}

		String action = (String)os.valueOf("a");
		String directory = (String)os.valueOf("o");

		tsrank = os.has("tsrank");

		if (tsrank) {
			System.out.println("ts ranking should be used...");
		}
		
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
						
			ic.setOption(IndexConfiguration.TSRANKING, tsrank);

			
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
			
			btcQueries(directory);
/*			
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
	*/	}
	}
	
	private static void btcQueries(String directory) throws Exception{
		
		// an index is accessed through an IndexReader object, which is passed
		// to all evaluators and searchers
		IndexReader ir = new IndexReader(new IndexDirectory(directory));
		
		
		// print all object properties in the data set
		// IndexReader has other methods which report information about the data,
		// e.g. cardinalities (some are not yet implemented)
		System.out.println(ir.getObjectProperties());
		
		// quad/triple retrieval with arbitrarily specified positions
		DataIndex di = ir.getDataIndex();
		Table<String> table = di.getQuads(null, "http://www.aktors.org/ontology/portal#name", "A.M. Campbell", null);
		System.out.println("type property quads: " + table);
		
		// create a structured query
		StructuredQuery q = new StructuredQuery("q1");
		
		// add edges to the query
		q.addEdge("?x", "http://www.aktors.org/ontology/portal#name", "A.M. Campbell");
	//	q.addEdge("?x", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", "?y");
		// <http://www.aktors.org/ontology/portal#name> "A.M. Campbell"
		// set select variables (currently has to be done after adding edges)
		q.setAsSelect("?x");

		// use an VPEvaluator to execute the query
		// VPEvaluator uses the data index only
		VPEvaluator eval = new VPEvaluator(ir);
		System.out.println("--------Results VPEvaluator-------");
		System.out.println(eval.evaluate(q));
		
		StructuredQuery q2 = new StructuredQuery("q2");
		q2.addEdge("?x","http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://www.aktors.org/ontology/portal#Affiliated-Person");
		q2.addEdge("?x", "http://www.aktors.org/ontology/portal#family-name", "?y");
		q2.setAsSelect("?x");
		q2.setAsSelect("?y");
		System.out.println("--------Results VPEvaluator-------");
		System.out.println(eval.evaluate(q2));
		
		testKeywordQuery(directory);
	}
	
	private static void testKeywordQuery(String directory) throws Exception {
		IndexReader ir = new IndexReader(new IndexDirectory(directory));
		DataIndex di = ir.getDataIndex();
		System.out.println(ir.getObjectProperties());
		
		System.out.println("-------------------------Test Simple Keywordquery-------------------------");

		String keywords = "daniel karlsruhe";
		System.out.println("Keywords:"+keywords);
		KeywordQuery kq = new KeywordQuery("q1", keywords);
		QueryParser luceneqp = new QueryParser(Constant.CONTENT_FIELD,
				new StandardAnalyzer());
		
		
		Query simplekq = luceneqp.parse(keywords);
		TopDocCollector topdoccoll = new TopDocCollector(25);
		IndexSearcher indexSearcher = new IndexSearcher(directory + "/values");
		if(tsrank){
			indexSearcher.setSimilarity(new TSSimilarity());
			System.out.println("Using TSSimilarity");

		}

		try {

			indexSearcher.search(simplekq, topdoccoll);

		} catch (IOException e) {

			System.out.println("Lucene Searching Exception: " + e.getMessage());

		}

		System.out.println("====Simple Keywordqueryresults====");
		System.out.println("Total results:" + topdoccoll.getTotalHits());

		TopDocs docs = topdoccoll.topDocs();
		ScoreDoc[] hits = docs.scoreDocs;
		
		
		
		for (ScoreDoc hit : hits) {
			Document doc = indexSearcher.doc(hit.doc);
			System.out.printf("%5.3f %s %s \n", hit.score, doc
					.get(Constant.ATTRIBUTE_FIELD), doc.get("uri"));
		
			
			Table<String> table = di.getQuads(doc.get("uri"), doc
					.get(Constant.ATTRIBUTE_FIELD), null, null);
			
			String[] row = table.getRows().iterator().next();
			String triple = null;
			
			
			
			for(String comp : row){
				if(triple != null)
				triple = triple+" "+comp;
				else
					triple = comp;
			}
			System.out.println("Matching triple:\n " + triple);
		
		}

		indexSearcher.close();

		System.out.println("======================");
		
	}
	
	
}
