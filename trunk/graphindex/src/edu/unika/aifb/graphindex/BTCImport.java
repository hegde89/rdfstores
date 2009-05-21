package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;

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
import edu.unika.aifb.graphindex.util.Util;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class BTCImport {
	private static final Logger log = Logger.getLogger(BTCImport.class);

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
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
//			gs.optimize();
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
	}
}
