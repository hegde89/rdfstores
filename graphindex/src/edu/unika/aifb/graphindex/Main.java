package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.data.VertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.importer.CompositeImporter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.DatasetAnalyzer;
import edu.unika.aifb.graphindex.preprocessing.TriplesPartitioner;
import edu.unika.aifb.graphindex.query.Query;
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
		
		if (args.length != 2) {
			log.error("Usage: Main <configfile> [partition|index|query]");
			return;
		}
		
		Map config = (Map)Yaml.load(new File(args[0]));
		
		String pVertexClass = (String)config.get("partitioning_vertexclass");
		String pCollectionClass = (String)config.get("partitioning_collectionclass");
		String iVertexClass = (String)config.get("indexing_vertexclass");
		String iCollectionClass = (String)config.get("indexing_collectionclass");
		
		String indexName = (String)config.get("index_name");
		String outputDirectory = (String)config.get("output_directory");
		String inputDirectory = (String)config.get("input_directory");
		
		String query = (String)config.get("query");
		
		String indexDirectory = new File(outputDirectory + "/" + indexName + "/index").getAbsolutePath(); 
		String graphDirectory = new File(outputDirectory + "/" + indexName + "/graph").getAbsolutePath();
		String componentDirectory = new File(outputDirectory + "/" + indexName + "/components").getAbsolutePath();
		String componentPrefix = componentDirectory + "/" + indexName;
		String hashesFile = componentPrefix + ".hashes";
		
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
		
		log.info("components output directory: " + componentDirectory);
		log.info("index output directory: " + indexDirectory);
		log.info("graph output directory: " + graphDirectory);
		log.info("hashes file: " + hashesFile);
		log.info("input files: " + ntFiles.size() + " nt, " + owlFiles.size() + " owl, total: " + (ntFiles.size() + owlFiles.size()));
		for (String file : ntFiles)
			log.info("  (nt)  " + file);
		for (String file : owlFiles)
			log.info("  (owl) " + file);
		
		if (args[1].equals("analyze") || args[1].equals("analyse")) {
			DatasetAnalyzer da = new DatasetAnalyzer();

			Importer importer = getImporter(ntFiles, owlFiles);
			importer.setTripleSink(da);
			importer.doImport();
			
			da.printAnalysis();
		}
		if (args[1].equals("partition")) {
			VertexFactory.setCollectionClass(Class.forName(pCollectionClass));
			VertexFactory.setVertexClass(Class.forName(pVertexClass));

			TriplesPartitioner tp = new TriplesPartitioner(componentPrefix);
			
			Importer importer = getImporter(ntFiles, owlFiles);
			importer.setTripleSink(tp);
			importer.doImport();
			
			tp.write(componentPrefix);
			
			tp = null;
			System.gc();
			
			VertexListBuilder vb = new VertexListBuilder(importer, componentPrefix);
			vb.write(componentPrefix);
		}
		if (args[1].equals("index")) {
			VertexFactory.setCollectionClass(Class.forName(iCollectionClass));
			VertexFactory.setVertexClass(Class.forName(iVertexClass));

			ExtensionStorage es = new LuceneExtensionStorage(indexDirectory);
			ExtensionManager em = new LuceneExtensionManager();
			em.setExtensionStorage(es);
			
			GraphStorage gs = new LuceneGraphStorage(graphDirectory);
			GraphManager gm = new GraphManagerImpl();
			gm.setGraphStorage(gs);
			
			StorageManager.getInstance().setExtensionManager(em);
			StorageManager.getInstance().setGraphManager(gm);
			em.initialize(true, false);
			gm.initialize(true, false);
			
			VertexListProvider vlp = new VertexListProvider(componentPrefix);
			HashValueProvider hvp = new HashValueProvider(hashesFile);
			
			FastIndexBuilder ib = new FastIndexBuilder(vlp, hvp);
			ib.buildIndex();
		}
		else if (args[1].equals("query")) {
			ExtensionStorage es = new LuceneExtensionStorage(indexDirectory);
			ExtensionManager em = new LuceneExtensionManager();
			em.setExtensionStorage(es);
			
			GraphStorage gs = new LuceneGraphStorage(graphDirectory);
			GraphManager gm = new GraphManagerImpl();
			gm.setGraphStorage(gs);
			
			StorageManager.getInstance().setExtensionManager(em);
			StorageManager.getInstance().setGraphManager(gm);
			em.initialize(false, true);
			gm.initialize(false, true);
			
			StructureIndex index = new StructureIndex();
			index.load();
			
			QueryParser qp = new QueryParser();
			Query q = qp.parseQuery(query);
			
			QueryEvaluator qe = new QueryEvaluator();
			qe.evaluate(q, index);
		}
	}
}
