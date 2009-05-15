package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.importer.CompositeImporter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.index.KeywordIndexBuilder;

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
		
		Set<String> _stages = new HashSet<String>(Arrays.asList("preprocess", "index", "query", "convert", "partition", "transform", "keywordindex"));
		
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
		String fwEdgeSetFile = (String)config.get("fw_edge_set_file");
		String bwEdgeSetFile = (String)config.get("bw_edge_set_file");
		
		String queryfile = (String)config.get("queryfile");
		int evalThreads = config.get("eval_threads") != null ? (Integer)config.get("eval_threads") : 10;
		int tableCacheSize = config.get("table_cache_size") != null ? (Integer)config.get("table_cache_size") : 100;
		int docCacheSize = config.get("doc_cache_size") != null ? (Integer)config.get("doc_cache_size") : 1000;
		int pathLength = config.get("path_length") != null ? (Integer)config.get("path_length") : -1;
		System.out.println(config);
		
		System.out.println(evalThreads + " " + tableCacheSize + " " + docCacheSize);
		
		String indexDirectory = new File(outputDirectory + "/index").getAbsolutePath(); 
		String graphDirectory = new File(outputDirectory + "/graph").getAbsolutePath();
		String componentDirectory = new File(outputDirectory + "/components").getAbsolutePath();
		String hashesFile = outputDirectory + "/hashes";
		String propertyHashesFile = outputDirectory + "/propertyhashes";
		
		List<String> ntFiles = new LinkedList<String>();
		List<String> owlFiles = new LinkedList<String>();
	
		Map<String,List<String>> files = (Map<String,List<String>>)config.get("input_files");
		
		if (indexName == null || outputDirectory == null || files == null || fwEdgeSetFile == null || bwEdgeSetFile == null) {
			log.error("The config file must at least contain index_name, output_directory, input_files, fw_edge_set_file and bw_edge_set_file.");
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
		
		Set<String> fwEdgeSet = new HashSet<String>();
		Set<String> bwEdgeSet = new HashSet<String>();
		
		if (fwEdgeSetFile != null)
			fwEdgeSet = Util.readEdgeSet(fwEdgeSetFile);
		if (bwEdgeSetFile != null)
			bwEdgeSet = Util.readEdgeSet(bwEdgeSetFile);
		
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
			Map options = new HashMap();
			options.put(StructureIndex.OPT_IGNORE_DATA_VALUES, true);
			options.put(StructureIndex.OPT_PATH_LENGTH, pathLength);
			iw.setOptions(options);
			iw.setForwardEdgeSet(fwEdgeSet);
			iw.setBackwardEdgeSet(bwEdgeSet);
			iw.setImporter(getImporter(ntFiles, owlFiles));
			iw.create(stages);
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
			StructureIndexReader index = new StructureIndexReader(outputDirectory);
			index.setNumEvalThreads(evalThreads);
			index.getIndex().setTableCacheSize(tableCacheSize);
			index.getIndex().setDocumentCacheSize(docCacheSize);
			
			QueryEvaluator qe = index.getQueryEvaluator();
			
			QueryLoader ql = new QueryLoader();
			List<Query> queries = ql.loadQueryFile(queryfile);
			
			start = System.currentTimeMillis();
			for (Query q : queries) {
				if (queryNames.size() == 0 || queryNames.contains(q.getName())) {
					log.debug("--------------------------------------------");
					log.debug("query: " + q.getName());
					log.debug(q);
					qe.evaluate(q);
					qe.clearCaches();
				}
			}
			
			index.close();
		}
		
//		if(stages.contains("keywordsearch")) {
//			KeywordSearchService ksService = new KeywordSearchService(outputDirectory);
//			Scanner scanner = new Scanner(System.in);
//			while (true) {
//				System.out.println("Please input the keywords:");
//				String line = scanner.nextLine();
//				String tokens[] = line.split(" ");
//				LinkedList<String> keywordList = new LinkedList<String>();
//				for (int i = 0; i < tokens.length; i++) {
//					keywordList.add(tokens[i]);
//				}
//				ksService.getPossibleGraphs(keywordList);
//			}
//			
//		}
		
		log.info("total time: " + (System.currentTimeMillis() - start) / 60000.0 + " minutes");
	}
}
