package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.LVertexM;
import edu.unika.aifb.graphindex.data.ListVertexCollection;
import edu.unika.aifb.graphindex.data.MapVertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.importer.ComponentImporter;
import edu.unika.aifb.graphindex.importer.HashingTripleConverter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.importer.TriplesImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.DatasetAnalyzer;
import edu.unika.aifb.graphindex.preprocessing.FileHashValueProvider;
import edu.unika.aifb.graphindex.preprocessing.SortedVertexListBuilder;
import edu.unika.aifb.graphindex.preprocessing.TripleConverter;
import edu.unika.aifb.graphindex.preprocessing.TriplesPartitioner;
import edu.unika.aifb.graphindex.preprocessing.VertexListBuilder;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryParser;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Literal;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.index.KeywordIndexBuilder;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public class Runner {

	private static final Logger log = Logger.getLogger(Runner.class);

	private static Importer getImporter(String dataset) {
		Importer importer = null;
		String datasetDir = "/data/datasets";
		if (dataset.equals("simple")) {
			importer = new NTriplesImporter();
			importer.addImport(datasetDir + "/simple.nt");
		}
		else if (dataset.equals("wordnet")) {
			importer = new NTriplesImporter();
			importer.addImport(datasetDir + "/wordnet.nt");
		}
		else if (dataset.equals("freebase")) {
			importer = new NTriplesImporter();
			importer.addImport(datasetDir + "/freebase_1m.nt");
		}
		else if (dataset.equals("lubm")) {
			importer = new OntologyImporter();
			for (File f : new File(datasetDir + "/lubm1/").listFiles()) {
				if (f.getName().startsWith("University")) {
					importer.addImport(f.getAbsolutePath());
//					break;
				}
			}
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more").listFiles())
//				if (f.getName().startsWith("University"))
//					importer.addImport(f.getAbsolutePath());
//			for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/more/muchmore").listFiles())
//				if (f.getName().startsWith("University"))
//					importer.addImport(f.getAbsolutePath());
		}
		else if (dataset.equals("swrc")) {
			importer = new OntologyImporter();
			importer.addImport(datasetDir + "/swrc/swrc_updated_v0.7.1.owl");
		}
		else if (dataset.equals("dbpedia")) {
			importer = new NTriplesImporter();
			importer.addImport(datasetDir + "/dbpedia/infobox_500k.nt");
		}
		else if (dataset.equals("sweto")) {
			importer = new NTriplesImporter();
			importer.addImport(datasetDir + "/swetodblp/swetodblp_april_2008-mod.nt");
		}
		else if (dataset.equals("simple_components")) {
			importer = new ComponentImporter();
			importer.addImport(datasetDir + "/components/simple");
		}
		else if (dataset.equals("chefmoz")) {
			importer = new RDFImporter();
			importer.addImport(datasetDir + "/chefmoz.rest.rdf");
		}
		return importer;
 	}
	
	private static void calculateCardinalities(StructureIndexReader ir) throws StorageException, IOException, InterruptedException, ExecutionException {
		QueryEvaluator qe = ir.getQueryEvaluator();
		QueryParser qp = new QueryParser();
		Map<String,Integer> cmap = new HashMap<String,Integer>();
		
		for (String edge : ir.getIndex().getBackwardEdges()) {
			Query q = qp.parseQuery("?x " + edge + " ?y");
			q.createQueryGraph(ir.getIndex());
			List<String[]> result = qe.evaluate(q);
			log.debug(edge + ": " + result.size());
			Set<String> values = new HashSet<String>();
			for (String[] row : result)
				values.add(row[q.getSelectVariables().indexOf("?y")]);
			log.debug(edge + ": " + result.size() + " => " + values.size());
			cmap.put(edge, values.size());
		}
		log.debug(cmap);
	}
	
	/**
	 * @param args
	 * @throws StorageException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws StorageException, IOException, InterruptedException, ExecutionException {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: convert, partition, transform, index or query")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("p", "prefix, determines index directory")
			.withRequiredArg().ofType(String.class).describedAs("prefix");
		op.accepts("d", "dataset, either lubm or dblp, determines queries")
			.withRequiredArg().ofType(String.class).describedAs("dataset");
		op.accepts("q", "optional, name of query")
			.withRequiredArg().ofType(String.class).describedAs("query name");
//		op.accepts("nodstes");
//		op.accepts("nosrces");
		op.accepts("k", "upper bound for label path length")
			.withRequiredArg().ofType(Integer.class).describedAs("path length");

		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("p") || !os.has("d")) {
			op.printHelpOn(System.out);
			return;
		}
		
		Set<String> stages = new HashSet<String>((Collection<? extends String>)os.valuesOf("a"));
		String prefix = (String)os.valueOf("p");
		String dataset = (String)os.valueOf("d");
		String queryName = (String)os.valueOf("q");
		int pathLength = os.valueOf("k") == null ? 10 : (Integer)os.valueOf("k");
		boolean dstUnmappedES = !os.has("nodstes");
		boolean srcUnmappedES = !os.has("nosrces");

		log.info("stages: " + stages);
		log.info("prefix: " + prefix);
		log.info("dataset: " + dataset);
		log.info("query name: " + queryName);
		
		String outputDirectory = "/data/sp/indexes/sp/" + prefix;
		
		long start = System.currentTimeMillis();
		if (stages.contains("convert") || stages.contains("partition") || stages.contains("transform") || stages.contains("index")) {
			Map options = new HashMap();
			options.put(StructureIndex.OPT_IGNORE_DATA_VALUES, true);
			options.put(StructureIndex.OPT_PATH_LENGTH, pathLength);
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, true);
			iw.setOptions(options);
			iw.setForwardEdgeSet(Util.readEdgeSet("/Users/gla/Projects/sp/datasets/" + dataset + ".fw.txt"));
			iw.setBackwardEdgeSet(Util.readEdgeSet("/Users/gla/Projects/sp/datasets/" + dataset + ".bw.txt"));
			iw.setImporter(getImporter(dataset));
			iw.create(stages);
//			iw.removeTemporaryFiles();
			iw.close();
		}
		
		if (stages.contains("keywordindex")) {
			KeywordIndexBuilder kb = new KeywordIndexBuilder(outputDirectory); 
			kb.indexKeywords();
		}
		
		if (stages.contains("temp")) {
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, false);
			iw.removeTemporaryFiles();
		}
		
		if (stages.contains("query")) {
			String queriesFile, queryOutputDirectory;
			List<String> queryFiles = new ArrayList<String>();
			if (dataset.equals("sweto")) {
				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/dblpeva.txt";
				queryOutputDirectory = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/dblpqueries/";
				String dir = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/dblp/";
//				queryFiles.add(dir + "AtomQuery_noResult.txt");
//				queryFiles.add(dir + "AtomQuery.txt");
//				queryFiles.add(dir + "StarQuery_noResult.txt");
				queryFiles.add(dir + "StarQuery.txt");
//				queryFiles.add(dir + "EntityQuery_noResult.txt");
//				queryFiles.add(dir + "EntityQuery.txt");
//				queryFiles.add(dir + "PathQuery_noResult.txt");
//				queryFiles.add(dir + "PathQuery.txt");
//				queryFiles.add(dir + "GraphQuery_noResult.txt");
//				queryFiles.add(dir + "GraphQuery.txt");
			}
			else if (dataset.equals("lubm")) {
				queriesFile = "/Users/gla/Studium/diplomarbeit/graphindex evaluation/lubmeva.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/AtomQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/EntityQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/PathQuery.txt";
//				queriesFile = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/vldb2/lubm/StarQuery.txt";
				queriesFile = "/Users/gla/Projects/sp/evaluation/queries/lubm/PathQuery.txt";
				queryOutputDirectory = "/Users/gl/Studium/diplomarbeit/graphindex evaluation/lubmqueries/";
			}
			else {
				log.error("unknown dataset");
				return;
			}
			
			StructureIndexReader index = new StructureIndexReader(outputDirectory);
			index.setNumEvalThreads(2);
			index.getIndex().setTableCacheSize(1);
			index.getIndex().setDocumentCacheSize(1000);
			
//			calculateCardinalities(index);
//			System.exit(-1);
			QueryLoader ql = index.getQueryLoader();
			List<Query> queries = new ArrayList<Query>();
			if (queryFiles != null && queryFiles.size() > 0) {
				for (String file : queryFiles)
					queries.addAll(ql.loadQueryFile(file));
			}
			else
				queries.addAll(ql.loadQueryFile(queriesFile));
			log.debug(queries.size());
			log.debug("bw: " + ql.getBackwardEdgeSet().size() + " " + ql.getBackwardEdgeSet());
			log.debug("fw: " + ql.getForwardEdgeSet().size() + " " +ql.getForwardEdgeSet());
			
//			for (Query q : queries) {
//				PrintWriter out = new PrintWriter(new FileWriter(queryOutputDirectory + q.getName() + ".txt"));
//				out.print(q.toSPARQL());
//				out.close();
//			}
			
			QueryEvaluator qe = index.getQueryEvaluator();
//			qe.getMLV().setDstExtSetup(dstUnmappedES, srcUnmappedES);
			
			String sizes = "";
			for (Query q : queries) {
				if (queryName != null && !queryName.equals(q.getName()))
					continue;
				log.debug("--------------------------------------------");
				log.debug("query: " + q.getName());
				log.debug(q);
//				q.createQueryGraph(index.getIndex());
				sizes += q.getName() + "\t" + qe.evaluate(q).size() + "\n";
				qe.clearCaches();
//				break;
			}
			System.out.println(sizes);
			index.close();
		}
		
		if (stages.contains("kwquery")) {
			KeywordSearcher ks = new KeywordSearcher(outputDirectory + "/keyword");
			List<String> queries = new ArrayList<String>();
			queries.add("professor");
			ks.searchKeywordElements(queries);
		}
		
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}
}
