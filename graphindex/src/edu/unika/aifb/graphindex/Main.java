package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.SortedVertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.data.VertexListBuilder;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.importer.CompositeImporter;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.TriplesImporter;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.preprocessing.DatasetAnalyzer;
import edu.unika.aifb.graphindex.preprocessing.TripleConverter;
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
		
		if (args.length < 2) {
			log.error("Usage: Main <configfile> [preprocess|index|query]");
			return;
		}
		
		Set<String> stages = new HashSet<String>();
		for (int i = 1; i < args.length; i++)
			stages.add(args[i]);
		
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
		
		String query = (String)config.get("query");
		
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
		
		if (stages.contains("analyze") || stages.contains("analyse")) {
			DatasetAnalyzer da = new DatasetAnalyzer();

			Importer importer = getImporter(ntFiles, owlFiles);
			importer.setTripleSink(da);
			importer.doImport();
			
			da.printAnalysis();
		}
		
		if (stages.contains("convert")) {
			log.info("stage: CONVERT");

			TripleConverter tc = new TripleConverter(outputDirectory);
			
			Importer importer = getImporter(ntFiles, owlFiles);
			importer.setTripleSink(tc);
			importer.doImport();
			
			tc.write();
			
			log.info("sorting...");
			Util.sortFile(outputDirectory + "/input.ht", outputDirectory + "/input_sorted.ht");
			log.info("sorting complete");
		}
		
		if (stages.contains("partition")) {
			log.info("stage: PARTITION");

			TriplesPartitioner tp = new TriplesPartitioner(componentDirectory);
			
			Importer importer = new TriplesImporter();
			importer.addImport(outputDirectory + "/input_sorted.ht");
			importer.setTripleSink(new ParsingTripleConverter(tp));
			importer.doImport();
			
			tp.write();
		}
		
		if (stages.contains("transform")) {
			log.info("stage: TRANSFORM");
			
			VertexFactory.setCollectionClass(Class.forName(pCollectionClass));
			VertexFactory.setVertexClass(Class.forName(pVertexClass));

			Importer importer = new TriplesImporter();
			importer.addImport(outputDirectory + "/input_sorted.ht");
			
			SortedVertexListBuilder vb = new SortedVertexListBuilder(importer, componentDirectory);
			vb.write();
		}
		
		if (stages.contains("index")) {
			log.info("stage: INDEX");
			
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
			
			VertexListProvider vlp = new VertexListProvider(componentDirectory);
			HashValueProvider hvp = new HashValueProvider(hashesFile, propertyHashesFile);
			
			FastIndexBuilder ib = new FastIndexBuilder(vlp, hvp);
			ib.buildIndex();
			
			em.close();
			gm.close();
		}
		
		if (stages.contains("query")) {
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
			
			em.close();
			gm.close();
		}
	}
}
