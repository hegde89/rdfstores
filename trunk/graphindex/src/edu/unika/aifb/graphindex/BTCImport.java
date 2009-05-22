package edu.unika.aifb.graphindex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.openrdf.model.vocabulary.RDFS;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.algorithm.largercp.LargeRCP;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NTriplesImporter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.index.KeywordIndexBuilder;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class BTCImport {
	private static final Logger log = Logger.getLogger(BTCImport.class);
	
	public static final int MAX_CACHE_SIZE = 1000000; 
	
	public static void main(String[] args) throws IOException, StorageException, EnvironmentLockedException, DatabaseException, InterruptedException {
		OptionParser op = new OptionParser();
		op.accepts("a", "action to perform, comma separated list of: import")
			.withRequiredArg().ofType(String.class).describedAs("action").withValuesSeparatedBy(',');
		op.accepts("f", "file to import")
			.withRequiredArg().ofType(String.class).describedAs("file");
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");
		op.accepts("bn", "remove bn");
		op.accepts("bdb", "bdb dir")
			.withRequiredArg().ofType(String.class).describedAs("bdb dir");
		
		OptionSet os = op.parse(args);
		
		if (!os.has("a") || !os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}
		
		String action = (String)os.valueOf("a");
        String file = (String)os.valueOf("f");
        String outputDirectory = (String)os.valueOf("o");
        boolean rmBN = os.has("bn");
        
        
        log.debug(Util.memory());
        
        if (action.equals("import")) {
			final LuceneGraphStorage gs = new LuceneGraphStorage(outputDirectory);
			gs.initialize(false, false);
			gs.setStoreGraphName(false);
			Importer importer = new NTriplesImporter(rmBN);
			importer.addImport(file);
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
			} catch (Exception e) {
				e.printStackTrace();
			}

			// gs.optimize();
			gs.close();
		}
		
		 if (action.equals("index")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(outputDirectory);
			gs.initialize(false, true);
			gs.setStoreGraphName(false);

			EnvironmentConfig config = new EnvironmentConfig();
			config.setTransactional(false);
			config.setAllowCreate(true);

			Set<String> edges = gs.getEdges();
			log.debug(Util.memory());
			log.debug(edges.size());

			new File(outputDirectory + "/bdb").mkdir();

			Environment env = new Environment(new File(outputDirectory + "/bdb"), config);

			LargeRCP rcp = new LargeRCP(gs, env, edges, edges);
			rcp.setIgnoreDataValues(true);
			rcp.createIndexGraph(5);

			gs.close();
			env.close();
		}
		
		if(action.equals("keywordindex")) {
			LuceneGraphStorage gs = new LuceneGraphStorage(outputDirectory);
			gs.initialize(false, true);
			gs.setStoreGraphName(false);
			
			prepareKeywordIndex(gs, outputDirectory + "/temp");
			
			EnvironmentConfig config = new EnvironmentConfig();
			config.setTransactional(false);
			config.setAllowCreate(true);

			Set<String> edges = gs.getEdges();
			log.debug(Util.memory());
			log.debug(edges.size());
			
			new File(outputDirectory + "/bdb").mkdir();
			
			Environment env = new Environment(new File(outputDirectory + "/bdb"), config);

			
			KeywordIndexBuilder kb = new KeywordIndexBuilder(outputDirectory, gs, env); 
			kb.indexKeywords();
			
			gs.close();
			env.close();
		}
	}
	
	public static void prepareKeywordIndex(LuceneGraphStorage gs,String outputDirectory) {
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
