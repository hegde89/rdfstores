package edu.unika.aifb.graphindex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.algorithm.largercp.LargeRCP;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.OntologyImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.indexing.FastIndexBuilder;
import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.IncrementalQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryEvaluator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.QueryLoader;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.index.KeywordIndexBuilder;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class BTCImport {
	private static final Logger log = Logger.getLogger(BTCImport.class);
	
	public static final int MAX_CACHE_SIZE = 1000000; 
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("bn", "remove bn");
		op.accepts("bdb", "bdb dir")
			.withRequiredArg().ofType(String.class).describedAs("bdb dir");
		op.accepts("qf", "query file")
			.withRequiredArg().ofType(String.class);
		op.accepts("q", "query name")
			.withRequiredArg().ofType(String.class);
		op.accepts("nk", "neighborhood distance")
			.withRequiredArg().ofType(Integer.class);
		op.accepts("bk", "bisim path length")
			.withRequiredArg().ofType(Integer.class);
		
		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
		String outputDirectory = (String)os.valueOf("o");
		boolean rmBN = os.has("bn");
		rmBN = false;
		
		String importDirectory = outputDirectory + "/tripleimport";
		String spDirectory = outputDirectory + "/sidx";
		String bdbDirectory = outputDirectory + "/bdb";
		String keywordIndexDirectory = outputDirectory + "/keyword";
		int neighborhoodDistance = os.has("nk") ? (Integer)os.valueOf("nk") : 2;
		int pathLength = os.has("bk") ? (Integer)os.valueOf("bk") : 10;
		
		log.debug(Util.memory());
		
		if (action.equals("optimize")) {
//			IndexWriter iw = new IndexWriter(FSDirectory.getDirectory(outputDirectory), new WhitespaceAnalyzer(), false);
//			iw.optimize();
//			iw.close();
		}
		
		if (action.equals("import")) {
			List<String> files = os.nonOptionArguments();
			
			if (files.size() == 1) {
				// check if file is a directory, if yes, import all files in the directory
				File f = new File(files.get(0));
				if (f.isDirectory()) {
					files = new ArrayList<String>();	
					for (File file : f.listFiles())
						files.add(file.getAbsolutePath());
				}
			}
			
			Importer importer;
			if (files.get(0).contains(".nt"))
				importer = new NTriplesImporter(rmBN);
			else if (files.get(0).contains(".owl"))
				importer = new OntologyImporter();
			else if (files.get(0).contains(".rdf") || files.get(0).contains(".xml"))
				importer = new RDFImporter();
			else
				throw new Exception("file type unknown");
			
			importer.addImports(files);
			
			final LuceneGraphStorage gs = new LuceneGraphStorage(importDirectory);
			gs.initialize(false, false);
			gs.setStoreGraphName(false);
			
			importer.setTripleSink(new TripleSink() {
				int triples = 0;
				public void triple(String s, String p, String o, String objectType) {
					try {
						gs.addEdge("btc", s, p, o);
						triples++;
						if (triples % 1000000 == 0)
							System.out.println(triples);
					} catch (StorageException e) {
						e.printStackTrace();
					}
				}
			});
			
			try {
				importer.doImport();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
			gs.close();
		}
		
		if (action.equals("dataprops")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(importDirectory);
			gs.initialize(false, true);
			
			Set<String> properties = gs.getEdges();
			Set<String> dataProperties = new HashSet<String>();
			for (String property : properties) {
				log.debug(property);
				if (!gs.hasEntityNodes(1, property))
					dataProperties.add(property);
			}
			
			PrintWriter pw = new PrintWriter(new FileWriter(outputDirectory + "/dataproperties"));
			for (String property : dataProperties)
				pw.println(property);
			pw.close();

			properties.removeAll(dataProperties);
			
			pw = new PrintWriter(new FileWriter(outputDirectory + "/properties"));
			for (String property : properties)
				pw.println(property);
			pw.close();
			
			log.debug("data properties: " + dataProperties.size() + ", properties: " + properties.size());
		}
		
		if (action.equals("index")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(importDirectory);
			gs.initialize(false, true);
			gs.setStoreGraphName(false);
			
			Set<String> edges = Util.readEdgeSet(outputDirectory + "/properties");
			log.debug(Util.memory());
			log.debug("properties: " + edges.size());
			
			EnvironmentConfig config = new EnvironmentConfig();
			config.setTransactional(false);
			config.setAllowCreate(true);

			Environment env = new Environment(new File(bdbDirectory), config);

			LargeRCP rcp = new LargeRCP(gs, env, edges, edges);
			rcp.setIgnoreDataValues(true);
			rcp.createIndexGraph(pathLength);

			gs.close();
			env.close();
		}
		
		if (action.equals("create")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(importDirectory);
			gs.initialize(false, true);
			gs.setStoreGraphName(false);
			
			Set<String> properties = Util.readEdgeSet(outputDirectory + "/properties");
			
			File bdb;
			if (os.has("bdb"))
				bdb = new File((String)os.valueOf("bdb"));
			else
				bdb = new File(outputDirectory + "/bdb");
			log.debug("bdb dir: " + bdb);
			
			EnvironmentConfig config = new EnvironmentConfig();
			config.setTransactional(false);
			config.setAllowCreate(false);

			Environment env = new Environment(bdb, config);
			BlockCache bc = new BlockCache(env);

			StructureIndexWriter idxWriter = new StructureIndexWriter(spDirectory, true);
			idxWriter.setBackwardEdgeSet(properties);
			idxWriter.setForwardEdgeSet(properties);
			Map options = new HashMap();
			options.put(StructureIndex.OPT_IGNORE_DATA_VALUES, true);
			options.put(StructureIndex.OPT_PATH_LENGTH, 10);
			idxWriter.setOptions(options);
			
			StructureIndex index = idxWriter.getIndex();

			ExtensionStorage es = index.getExtensionManager().getExtensionStorage();
			GraphStorage igs = index.getGraphManager().getGraphStorage();
			
			Set<String> indexEdges = new HashSet<String>();
			int triples = 0;
			for (String property : properties) {
				for (Iterator<String[]> ti = gs.iterator(property); ti.hasNext(); ) {
					String[] triple = ti.next();
					String s = triple[0];
					String o = triple[2];
					
					if (!Util.isEntity(o))
						continue;
					
					String subExt = bc.getBlockName(s);
					String objExt = bc.getBlockName(o);
					
					// build index graph
					String indexEdge = new StringBuilder().append(subExt).append("__").append(property).append("__").append(objExt).toString();
					if (indexEdges.add(indexEdge)) {
						igs.addEdge("g1", subExt, property, objExt);
					}
					
					// add triples to extensions
					es.addTriples(IndexDescription.PSESO, subExt, property, s, Arrays.asList(o));
					es.addTriples(IndexDescription.POESS, subExt, property, o, Arrays.asList(s));
					es.addData(IndexDescription.PSES, es.concat(new String[] { property, s }, 2), Arrays.asList(subExt) , false);
					es.addData(IndexDescription.POES, es.concat(new String[] { property, o }, 2), Arrays.asList(subExt) , false);
					
					triples++;
					
					if (triples % 100000 == 0)
						log.debug("triples: " + triples);
				}
			}
			
			log.debug("index graph edges: " + indexEdges.size());
			
			index.addIndex(IndexDescription.PSESO);
			index.addIndex(IndexDescription.POESS);
			index.addIndex(IndexDescription.PSES);
			index.addIndex(IndexDescription.POES);
			
			igs.optimize();
			
			idxWriter.close();
		}
		
		if (action.equals("mergedocs")) {
			LuceneExtensionStorage les = new LuceneExtensionStorage(spDirectory + "/index");
			les.initialize(false, false);

			for (IndexDescription index : Arrays.asList(IndexDescription.PSESO, IndexDescription.POESS, IndexDescription.PSES, IndexDescription.POES)) {
				log.debug(index.getIndexFieldName());
				les.mergeIndexDocuments(index);
			}
			
			log.debug("optimizing");
			les.optimize();
			
			les.close();
		}
		
		if (action.equals("query")) {
			String queryFile = (String)os.valueOf("qf");
			String queryName = (String)os.valueOf("q");
			
			if (queryFile == null) {
				log.error("no query file specified");
			}
			
			QueryLoader loader = new QueryLoader();
			List<Query> queries = loader.loadQueryFile(queryFile);
			
			StructureIndexReader reader = new StructureIndexReader(spDirectory);
			
			IQueryEvaluator qe = new IncrementalQueryEvaluator(reader, keywordIndexDirectory);
			for (Query q : queries) {
				if (queryName != null && !q.getName().equals(queryName))
					continue;
				
				List<String[]> results = qe.evaluate(q);
				log.info("query " + q.getName() + ": " + results.size() + " results");
			}
			
			reader.close();
		}
		
		if (action.equals("keywordindex")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(importDirectory);
			gs.initialize(false, true);
			gs.setStoreGraphName(false);
			
			prepareKeywordIndex(gs, outputDirectory + "/temp");
			
			EnvironmentConfig config = new EnvironmentConfig();
			config.setTransactional(false);
			config.setAllowCreate(true);

			Environment env = new Environment(new File(bdbDirectory), config);
			
			KeywordIndexBuilder kb = new KeywordIndexBuilder(outputDirectory, gs, env, neighborhoodDistance); 
			kb.indexKeywords();
			
			gs.close();
			env.close();
		}
	}
	
	public static void prepareKeywordIndex(LuceneGraphStorage gs, String outputDirectory) {
		File file = new File(outputDirectory);
		if(!file.exists())
			file.mkdirs();
		IndexReader indexReader = gs.getIndexSearcher().getIndexReader();

		TreeSet<String> conSet = new TreeSet<String>();
		TreeSet<String> relSet = new TreeSet<String>();
		TreeSet<String> attrSet = new TreeSet<String>();
		TreeSet<String> entSet = new TreeSet<String>();

		int triples = 0;

		try {
			for (int i = 0; i < indexReader.numDocs(); i++) {
				Document doc = indexReader.document(i);
				if (doc != null) {
					String s = doc.get(LuceneGraphStorage.FIELD_SRC);
					String p = doc.get(LuceneGraphStorage.FIELD_EDGE);
					String o = doc.get(LuceneGraphStorage.FIELD_DST);

					if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.RELATION)) {
						relSet.add(p);
						entSet.add(s);
						entSet.add(o);
						if (entSet.size() > MAX_CACHE_SIZE) {
							writeEntities(entSet, outputDirectory);
						}
					} else if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.LITERAL)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.ATTRIBUTE)) {
						attrSet.add(p);
						entSet.add(s);
						if (entSet.size() > MAX_CACHE_SIZE) {
							writeEntities(entSet, outputDirectory);
						}
					} else if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.CONCEPT)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.TYPE)) {
						conSet.add(o);
						entSet.add(s);
						if (entSet.size() > MAX_CACHE_SIZE) {
							writeEntities(entSet, outputDirectory);
						}
					}
					triples++;
					if (triples % 1000000 == 0)
						System.out.println(triples);
				}
			}
			writeAll(conSet, relSet, attrSet, entSet, outputDirectory);
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	public static void writeEntities(Set<String> entSet, String outputDirectory) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/entities", true)));
		for (String ent : entSet) {
			out.println(ent);
		}
		entSet.clear();
		out.close();
	}
	
	public static void writeAll(Set<String> conSet, Set<String> relSet, Set<String> attrSet, Set<String> entSet, String outputDirectory) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/concepts", true)));
		for (String con : conSet) {
			out.println(con);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/relations", true)));
		for (String rel : relSet) {
			out.println(rel);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/attributes", true)));
		for (String attr : attrSet) {
			out.println(attr);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/entities", true)));
		for (String ent : entSet) {
			out.println(ent);
		}
		out.close();
	}
	
	public static void removeTemporaryFiles(String outputDirectory) {
		File f = new File(outputDirectory + "/attributes");
		f.delete();
		
		f = new File(outputDirectory + "/relations");
		f.delete();
		
		f = new File(outputDirectory + "/concepts");
		f.delete();
		
		f = new File(outputDirectory + "/entities");
		f.delete();
	}
}
