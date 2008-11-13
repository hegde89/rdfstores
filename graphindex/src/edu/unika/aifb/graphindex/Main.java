package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.importer.CompositeImporter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
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
import edu.unika.aifb.graphindex.query.model.Query;
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

public class Main {

	private static final Logger log = Logger.getLogger(Main.class);
	
	private static Importer getImporter(List<String> ntFiles, List<String> owlFiles) {
		CompositeImporter ci = new CompositeImporter();
		if (ntFiles.size() > 0) {
			NTriplesImporter nti = new NTriplesImporter();
			nti.addImports(ntFiles);
			ci.addImporter(nti);
		}
		
		if (owlFiles.size() > 0) {
			OntologyImporter oi = new OntologyImporter();
			oi.addImports(owlFiles);
			ci.addImporter(oi);
		}
		return ci;
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws StorageException, IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		
		if (args.length < 2) {
			log.error("Usage: Main <configfile> [preprocess|index|query]");
			return;
		}
		
		Set<String> _stages = new HashSet<String>(Arrays.asList("preprocess", "index", "query", "convert", "partition", "transform"));
		
		Set<String> stages = new HashSet<String>();
		Set<String> queryNames = new HashSet<String>();
		for (int i = 1; i < args.length; i++) {
			if (_stages.contains(args[i]))
				stages.add(args[i]);
			else
				queryNames.add(args[i]);
		}
		
		if (stages.contains("preprocess")) {
			stages.add("convert");
			stages.add("partition");
			stages.add("transform");
		}
		
		log.info("stages: " + stages);
		
		Map config = (Map)Yaml.load(new File(args[0]));
		
		String pVertexClass = (String)config.get("partitioning_vertexclass");
		String pCollectionClass = (String)config.get("partitioning_collectionclass");
		String iVertexClass = (String)config.get("indexing_vertexclass");
		String iCollectionClass = (String)config.get("indexing_collectionclass");
		
		String indexName = (String)config.get("index_name");
		String outputDirectory = (String)config.get("output_directory") + "/" + indexName;
		String inputDirectory = (String)config.get("input_directory");
		
		String queryfile = (String)config.get("queryfile");
		int evalThreads = config.get("eval_threads") != null ? (Integer)config.get("eval_threads") : 10;
		int tableCacheSize = config.get("table_cache_size") != null ? (Integer)config.get("table_cache_size") : 100;
		int docCacheSize = config.get("doc_cache_size") != null ? (Integer)config.get("doc_cache_size") : 1000;
		
		String indexDirectory = new File(outputDirectory + "/index").getAbsolutePath(); 
		String graphDirectory = new File(outputDirectory + "/graph").getAbsolutePath();
		String componentDirectory = new File(outputDirectory + "/components").getAbsolutePath();
		String hashesFile = outputDirectory + "/hashes";
		String propertyHashesFile = outputDirectory + "/propertyhashes";
		
		List<String> ntFiles = new LinkedList<String>();
		List<String> owlFiles = new LinkedList<String>();
	
		Map<String,List<String>> files = (Map<String,List<String>>)config.get("input_files");
		
		if (indexName == null || outputDirectory == null || files == null) {
			log.error("The config file must at least contain index_name, output_directory and input_files.");
			return;
		}
		
		if (files.get("nt") != null) {
			for (String fileName : files.get("nt")) {
				File f = new File(fileName);
				if (f.isAbsolute() || inputDirectory == null)
					ntFiles.add(fileName);
				else 
					ntFiles.add(inputDirectory + "/" + fileName);
			}
		}
		
		if (files.get("owl") != null) {
			for (String fileName : files.get("owl")) {
				File f = new File(fileName);
				if (f.isAbsolute() || inputDirectory == null)
					owlFiles.add(fileName);
				else 
					owlFiles.add(inputDirectory + "/" + fileName);
			}
		}
		
		log.info("output directory: " + outputDirectory);
		log.info("components output directory: " + componentDirectory);
		log.info("index output directory: " + indexDirectory);
		log.info("graph output directory: " + graphDirectory);
		log.info("hashes file: " + hashesFile);
		log.info("input files: " + ntFiles.size() + " nt, " + owlFiles.size() + " owl, total: " + (ntFiles.size() + owlFiles.size()));
		for (String file : ntFiles)
			log.info("  (nt)  " + file);
		for (String file : owlFiles)
			log.info("  (owl) " + file);
		
		long start = System.currentTimeMillis();
		
		if (stages.contains("convert") || stages.contains("partition") || stages.contains("transform") || stages.contains("index")) {
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, true);
			iw.setImporter(getImporter(ntFiles, owlFiles));
			iw.create(stages);
//			iw.removeTemporaryFiles();
			iw.close();
		}
		
		if (stages.contains("temp")) {
			StructureIndexWriter iw = new StructureIndexWriter(outputDirectory, false);
			iw.removeTemporaryFiles();
		}
		
		if (stages.contains("query")) {
			StructureIndexReader index = new StructureIndexReader(outputDirectory);
			index.setNumEvalThreads(evalThreads);
			index.getIndex().setTableCacheSize(tableCacheSize);
			index.getIndex().setDocumentCacheSize(docCacheSize);
			
			QueryEvaluator qe = index.getQueryEvaluator();
			
			QueryLoader ql = new QueryLoader();
			List<Query> queries = ql.loadQueryFile(queryfile);
			
			start = System.currentTimeMillis();
			for (Query q : queries) {
				log.debug("--------------------------------------------");
				log.debug("query: " + q.getName());
				log.debug(q);
				if (queryNames.size() == 0 || queryNames.contains(q.getName()))
					qe.evaluate(q);
			}
			
			index.close();
		}
		
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}
}
